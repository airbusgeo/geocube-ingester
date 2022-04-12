package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube/interface/storage/uri"
	"go.uber.org/zap/zapcore"
)

const (
	burstSwath   = "swath"
	tileNumber   = "number"
	burstCohDate = "cohdate" // Date of the reference burst if different from previous date or date of the burst
	sceneName    = "scene"
	sceneDate    = "date"

	python  = "python"
	snap    = "snap"
	command = "cmd"

	snapDateFormat = "02Jan2006"
)

// TileCondition is a condition on tiles to do an action (execute a step, create a file...)
type TileCondition struct {
	Name string
	Pass func([]common.Tile) bool
}

// condPass is a tileCondition always true
var pass = TileCondition{"pass", func(tiles []common.Tile) bool { return true }}

// condDiffTile returns true if tile1 != tile2
var condDiffT0T1 = TileCondition{"different_T0_T1", func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID != tiles[1].Scene.SourceID }}
var condDiffT0T2 = TileCondition{"different_T0_T2", func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID != tiles[2].Scene.SourceID }}
var condDiffT1T2 = TileCondition{"different_T1_T2", func(tiles []common.Tile) bool { return tiles[1].Scene.SourceID != tiles[2].Scene.SourceID }}

// condEqualTile returns true if tile1 == tile2
var condEqualT0T1 = TileCondition{"equal_T0_T1", func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID == tiles[1].Scene.SourceID }}
var condEqualT0T2 = TileCondition{"equal_T0_T2", func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID == tiles[2].Scene.SourceID }}
var condEqualT1T2 = TileCondition{"equal_T1_T2", func(tiles []common.Tile) bool { return tiles[1].Scene.SourceID == tiles[2].Scene.SourceID }}

var tileConditionJSON = map[string]TileCondition{
	pass.Name:          pass,
	condDiffT0T1.Name:  condDiffT0T1,
	condDiffT0T2.Name:  condDiffT0T2,
	condDiffT1T2.Name:  condDiffT1T2,
	condEqualT0T1.Name: condEqualT0T1,
	condEqualT0T2.Name: condEqualT0T2,
	condEqualT1T2.Name: condEqualT1T2,
}

type Arg interface{}

type ArgIn struct { // input of the graph
	Input     int               `json:"tile_index"` // Index of input [0, 1, 2]
	Layer     service.Layer     `json:"layer"`
	Extension service.Extension `json:"extension"`
}
type ArgOut struct { // output of the graph
	service.Layer `json:"layer"`
	Extension     service.Extension `json:"extension"`
}
type ArgFixed string  // fixed arg
type ArgConfig string // arg from config
type ArgTile string   // arg from tile info

type ProcessingStep struct {
	Engine    string // Python or Snap
	Command   string
	Args      map[string]Arg
	Condition TileCondition
}

// DType of an output file
type DType int32

// DType of an output file
const (
	Undefined DType = iota
	UInt8
	UInt16
	UInt32
	Int16
	Int32
	Float32
	Float64
	Complex64
)

func DTypeFromString(dtype string) DType {
	switch strings.ToLower(dtype) {
	default:
		return Undefined
	case "uint8", "byte", "u1":
		return UInt8
	case "uint16", "u2":
		return UInt16
	case "uint32", "u4":
		return UInt32
	case "int16", "i2":
		return Int16
	case "int32", "i4":
		return Int32
	case "float32", "f4":
		return Float32
	case "float64", "f8":
		return Float64
	case "complex64", "c4":
		return Complex64
	}
}

// OutFileAction
type OutFileAction int32

// OutFileAction
const (
	ToIgnore OutFileAction = iota
	ToCreate
	ToIndex
	ToDelete
)

// File is a layer with an extension
type File struct {
	Layer     service.Layer     `json:"layer"`
	Extension service.Extension `json:"extension"`
}

// InFile describes an input file of the processing
type InFile struct {
	File
	Condition TileCondition `json:"condition"`
}

// OutFile describes an output file of the processing
type OutFile struct {
	File
	dformatOut Arg           // argFixed or argConfig
	DType      DType         `json:"datatype"`
	NoData     float64       `json:"nodata"`
	Min        float64       `json:"min_value"`
	Max        float64       `json:"max_value"`
	ExtMin     float64       `json:"ext_min_value"`
	ExtMax     float64       `json:"ext_max_value"`
	Exponent   float64       `json:"exponent"`
	Nbands     int           `json:"nbands"`
	Action     OutFileAction `json:"action"`
	Condition  TileCondition `json:"condition"`
}

func newOutFile(layer service.Layer, ext service.Extension, dformatOut Arg, realmin, realmax, exponent float64, nbands int, status OutFileAction, condition TileCondition) OutFile {
	return OutFile{
		File: File{
			Layer:     layer,
			Extension: ext,
		},
		dformatOut: dformatOut,
		ExtMin:     realmin,
		ExtMax:     realmax,
		Exponent:   exponent,
		Action:     status,
		Condition:  condition,
		Nbands:     nbands,
	}
}

func (of *OutFile) setDFormatOut(config GraphConfig) error {
	if of.dformatOut == nil {
		return nil
	}
	dformatOutS, err := formatArgs(of.dformatOut, config, []common.Tile{})
	if err != nil {
		return fmt.Errorf("setDFormatOut.%w", err)
	}

	dformatOut := strings.Split(dformatOutS, ",")
	if len(dformatOut) != 4 {
		return fmt.Errorf("setDFormatOut : invalid dformatOut %s. Expecting dtype,nodata,min,max. %w", dformatOut, err)
	}

	of.DType = DTypeFromString(dformatOut[0])
	if of.NoData, err = strconv.ParseFloat(dformatOut[1], 64); err != nil {
		return fmt.Errorf("setDFormatOut : invalid dformatOut.Nodata %s. Expecting dtype,nodata,min,max. %w", dformatOut, err)
	}
	if of.Min, err = strconv.ParseFloat(dformatOut[2], 64); err != nil {
		return fmt.Errorf("setDFormatOut : invalid dformatOut.Min %s. Expecting dtype,nodata,min,max. %w", dformatOut, err)
	}
	if of.Max, err = strconv.ParseFloat(dformatOut[3], 64); err != nil {
		return fmt.Errorf("setDFormatOut : invalid dformatOut.Max %s. Expecting dtype,nodata,min,max. %w", dformatOut, err)
	}
	return nil
}

// GraphConfig is a configuration map for a processing graph
type GraphConfig map[string]string

// ProcessingGraph is a set of steps
type ProcessingGraph struct {
	steps    []ProcessingStep
	snap     string
	InFiles  [3][]InFile
	outFiles [][]OutFile
}

func (g *ProcessingGraph) Summary() string {
	s := fmt.Sprintf("- %d steps\n", len(g.steps))
	for _, step := range g.steps {
		s += fmt.Sprintf("   * %s\n", step.Command)
	}
	s += fmt.Sprintf("- %d inputs files\n", len(g.InFiles))
	for i, fs := range g.InFiles {
		for _, f := range fs {
			s += fmt.Sprintf("   * %-10s[%d] (%v)\n", f.Layer, i, f.Condition.Name)
		}
	}
	s += fmt.Sprintf("- %d outputs files\n", len(g.outFiles))
	for i, fs := range g.outFiles {
		for _, f := range fs {
			switch f.Action {
			case ToCreate:
				s += fmt.Sprintf("   + %-10s[%d] (%v)\n", f.Layer, i, f.Condition.Name)
			case ToIndex:
				s += fmt.Sprintf("   i+ %-10s[%d] (%v)\n", f.Layer, i, f.Condition.Name)
			case ToDelete:
				s += fmt.Sprintf("   - %-10s[%d] (%v)\n", f.Layer, i, f.Condition.Name)
			default:
				s += fmt.Sprintf("   ? %-10s[%d] (%v)\n", f.Layer, i, f.Condition.Name)
			}
		}
	}
	return s
}

func fileExists(cwd, file string) (string, error) {
	if _, err := os.Stat(file); err == nil || !errors.Is(err, os.ErrNotExist) || cwd == "" {
		return file, err
	}
	file = path.Join(cwd, file)
	return fileExists("", file)
}

func newProcessingGraph(snapPath string, steps []ProcessingStep, infiles [3][]InFile, outfiles [][]OutFile) (*ProcessingGraph, error) {
	// Check commands
	graphPath := Getenv("GRAPHPATH", "/data/graph")
	snapRequired := false
	for i, step := range steps {
		cmd, err := fileExists(graphPath, step.Command)
		if err != nil {
			return nil, fmt.Errorf("newProcessingGraph: Command not found: %s", step.Command)
		}
		steps[i].Command = cmd
		if step.Engine == snap {
			snapRequired = true
		}
	}

	if snapRequired {
		if _, err := os.Stat(snapPath); err != nil {
			return nil, fmt.Errorf("newProcessingGraph: SNAP not found: %s", snapPath)
		}
	}

	return &ProcessingGraph{
		snap:     snapPath,
		steps:    steps,
		InFiles:  infiles,
		outFiles: outfiles,
	}, nil
}

// LoadGraph returns the graph from its name and its default configuration
func LoadGraph(ctx context.Context, graphName string) (*ProcessingGraph, GraphConfig, error) {
	switch graphName {
	case "S1Preprocessing":
		g, err := newS1PreProcessingGraph()
		if err != nil {
			return nil, nil, err
		}
		return g, S1DefaultConfig(), nil
	case "S1BackscatterCoherence":
		g, err := newS1BsCohGraph()
		if err != nil {
			return nil, nil, err
		}
		return g, S1DefaultConfig(), nil
	case "S1CoregExtract":
		g, err := newS1CoregExtractGraph()
		if err != nil {
			return nil, nil, err
		}
		return g, S1DefaultConfig(), nil
	case "S1Clean":
		g, err := newS1CleanGraph()
		if err != nil {
			return nil, nil, err
		}
		return g, S1DefaultConfig(), nil
	}

	return LoadGraphFromFile(ctx, graphName)
}

// LoadGraphFromFile returns the graph from a filename
func LoadGraphFromFile(ctx context.Context, graphFile string) (*ProcessingGraph, GraphConfig, error) {
	f, err := fileExists(Getenv("GRAPHPATH", "/data/graph"), graphFile)
	if err != nil {
		// Try to download it
		graphFileUri, e := uri.ParseUri(graphFile)
		if e != nil {
			return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: unknown graph (%w-%v)", graphFile, err, e)
		}
		graphFilePath, err := ioutil.TempFile("", "")
		if err != nil {
			return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: unable to create temp file: %w", graphFile, err)
		}
		graphFilePath.Close()
		defer os.Remove(graphFilePath.Name())
		if err = graphFileUri.DownloadToFile(ctx, graphFilePath.Name()); err != nil {
			return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: unable to download graph: %w", graphFile, err)
		}

		return LoadGraphFromFile(ctx, graphFilePath.Name())
	}
	graphFile = f
	jsonFile, err := os.Open(graphFile)
	if err != nil {
		return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: %w", graphFile, err)
	}
	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: %w", graphFile, err)
	}

	var graphJSON ProcessingGraphJSON
	if err := json.Unmarshal(byteValue, &graphJSON); err != nil {
		return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: %w", graphFile, err)
	}

	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")
	graph, err := newProcessingGraph(snapPath, graphJSON.Steps, graphJSON.InFiles, graphJSON.OutFiles)
	if err != nil {
		return nil, nil, fmt.Errorf("LoadGraphFromFile[%s]: %w", graphFile, err)
	}
	if graphJSON.Config == nil {
		graphJSON.Config = map[string]string{}
	}

	return graph, graphJSON.Config, nil
}

// S1DefaultConfig returns a basic configuration
func S1DefaultConfig() GraphConfig {
	return GraphConfig{
		"snap_cpu_parallelism":       "1",
		"terrain_correction_range":   "4",
		"terrain_correction_azimuth": "1",
		"coherence_range":            "16",
		"coherence_azimuth":          "4",
		"dem_name":                   "SRTM 3Sec",
		"dem_file":                   "",
		"dem_nodata":                 "0",
		"dem_egm_correction":         "True",
		"dem_resampling":             "BILINEAR_INTERPOLATION",
		"img_resampling":             "BICUBIC_INTERPOLATION",
		"bkg_resampling":             "BISINC_21_POINT_INTERPOLATION",
		"resolution":                 "20",
		"projection":                 "EPSG:4326",
		"bs_erode_iterations":        "10",
		"coh_erode_iterations":       "10",
		"dformat_out":                "float32,0,0,1", // option to map float32[0,1] to another lighter format (ie. int16,-32768,0,1000)
	}
}

// Getenv retrieves the value of the environment variable named by the key.
// Return def if not set
func Getenv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

// newS1PreProcessingGraph creates a new preprocessing graph for S1 (to use with )
func newS1PreProcessingGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")

	// Define inputs
	infiles := [3][]InFile{}

	// Define outputs
	outfiles := [][]OutFile{
		{newOutFile(service.LayerPreprocessed, service.ExtensionDIMAP, ArgFixed("float32,0,0,1"), 0, 1, 1, 1, ToCreate, pass)},
		{},
		{},
	}

	// Create processing steps
	steps := []ProcessingStep{
		// Extract burst from image and preprocess
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_BurstSplit_AO_CAL.xml"),
			Condition: pass,

			Args: map[string]Arg{
				"input":  ArgTile(sceneName),
				"output": ArgOut{service.LayerPreprocessed, service.ExtensionDIMAP},
				"swath":  ArgTile(burstSwath),
				"polar":  ArgFixed("\"VV VH\""),
				"burst":  ArgTile(tileNumber),
			},
		},
	}

	return newProcessingGraph(snapPath, steps, infiles, outfiles)
}

// newS1BsCohGraph creates a new processing graph to compute Backscatter and Coherence of S1 images
func newS1BsCohGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")

	// Define inputs
	infiles := [3][]InFile{
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, pass}},
		{{File{service.LayerCoregExtract, service.ExtensionDIMAP}, condDiffT1T2}},
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, condDiffT0T2}},
	}

	// Define outputs
	outfiles := [][]OutFile{
		{
			newOutFile(service.LayerBackscatterVV, service.ExtensionGTiff, ArgConfig("dformat_out"), 0, 1, 1, 1, ToIndex, pass),
			newOutFile(service.LayerBackscatterVH, service.ExtensionGTiff, ArgConfig("dformat_out"), 0, 1, 1, 1, ToIndex, pass),
			newOutFile(service.LayerCoherenceVV, service.ExtensionGTiff, ArgConfig("dformat_out"), 0, 1, 1, 1, ToIndex, condDiffT0T1),
			newOutFile(service.LayerCoherenceVH, service.ExtensionGTiff, ArgConfig("dformat_out"), 0, 1, 1, 1, ToIndex, condDiffT0T1),
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: ToCreate, Condition: condDiffT0T1},
			{File: File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Action: ToDelete, Condition: condDiffT0T1},
		},
		{
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: ToDelete, Condition: condDiffT1T2},
		},
		{},
	}

	// Create processing steps
	steps := []ProcessingStep{
		// Coregistration with ref burst
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_BkG.xml"),
			Condition: pass,

			Args: map[string]Arg{
				"master":         ArgIn{Input: 2, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"slave":          ArgIn{Input: 0, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"output":         ArgOut{service.LayerCoregistrated, service.ExtensionDIMAP},
				"dem_name":       ArgConfig("dem_name"),
				"dem_file":       ArgConfig("dem_file"),
				"dem_nodata":     ArgConfig("dem_nodata"),
				"dem_resampling": ArgConfig("dem_resampling"),
				"resampling":     ArgConfig("bkg_resampling"),
			},
		},

		// Extraction of coregistred slave
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_SlvExtract.xml"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"input":  ArgIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"output": ArgOut{service.LayerCoregExtract, service.ExtensionDIMAP},
			},
		},

		// Backscatter computation
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_Deb_BetaSigma_ML_TC_RNKELL.xml"),
			Condition: pass,

			Args: map[string]Arg{
				"input":             ArgIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"outputVV":          ArgOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"outputVH":          ArgOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"range_multilook":   ArgConfig("terrain_correction_range"),
				"azimuth_multilook": ArgConfig("terrain_correction_azimuth"),
				"dem_name":          ArgConfig("dem_name"),
				"dem_file":          ArgConfig("dem_file"),
				"dem_nodata":        ArgConfig("dem_nodata"),
				"dem_egm":           ArgConfig("dem_egm_correction"),
				"dem_resampling":    ArgConfig("dem_resampling"),
				"img_resampling":    ArgConfig("img_resampling"),
				"projection":        ArgConfig("projection"),
				"resolution":        ArgConfig("resolution"),
				"grid_align":        ArgFixed("true"),
				"band":              ArgFixed("Sigma0"),
				"trig":              ArgFixed("sin"),
				"swath":             ArgTile(burstSwath),
				"img_suffix":        ArgTile(sceneDate),
			},
		},

		{
			Engine:    python,
			Command:   path.Join(python, "erodeMask.py"),
			Condition: pass,

			Args: map[string]Arg{
				"file-in":    ArgOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"file-out":   ArgOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"no-data":    ArgFixed("0"),
				"iterations": ArgConfig("bs_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(python, "convert.py"),
			Condition: pass,

			Args: map[string]Arg{
				"file-in":     ArgOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"file-out":    ArgOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"range-in":    ArgFixed("0,1"),
				"dformat-out": ArgConfig("dformat_out"),
			},
		},

		{
			Engine:    python,
			Command:   path.Join(python, "erodeMask.py"),
			Condition: pass,

			Args: map[string]Arg{
				"file-in":    ArgOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"file-out":   ArgOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"no-data":    ArgFixed("0"),
				"iterations": ArgConfig("bs_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(python, "convert.py"),
			Condition: pass,

			Args: map[string]Arg{
				"file-in":     ArgOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"file-out":    ArgOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"range-in":    ArgFixed("0,1"),
				"dformat-out": ArgConfig("dformat_out"),
			},
		},

		// Coregistration with prev burst
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_BkG.xml"),
			Condition: condDiffT1T2,

			Args: map[string]Arg{
				"master":         ArgIn{Input: 0, Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP},
				"slave":          ArgIn{Input: 1, Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP},
				"output":         ArgOut{service.LayerCoregistrated, service.ExtensionDIMAP},
				"dem_name":       ArgConfig("dem_name"),
				"dem_file":       ArgConfig("dem_file"),
				"dem_nodata":     ArgConfig("dem_nodata"),
				"dem_resampling": ArgConfig("dem_resampling"),
				"resampling":     ArgConfig("bkg_resampling"),
			},
		},

		// Coherence computation
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_Coh_BSel_Deb_ML_TC.xml"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"input":             ArgIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"outputVV":          ArgOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"outputVH":          ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"coherence_range":   ArgConfig("coherence_range"),
				"coherence_azimuth": ArgConfig("coherence_azimuth"),
				"sel_date":          ArgTile(burstCohDate),
				"range_multilook":   ArgConfig("terrain_correction_range"),
				"azimuth_multilook": ArgConfig("terrain_correction_azimuth"),
				"dem_name":          ArgConfig("dem_name"),
				"dem_file":          ArgConfig("dem_file"),
				"dem_nodata":        ArgConfig("dem_nodata"),
				"dem_egm":           ArgConfig("dem_egm_correction"),
				"dem_resampling":    ArgConfig("dem_resampling"),
				"img_resampling":    ArgConfig("img_resampling"),
				"projection":        ArgConfig("projection"),
				"resolution":        ArgConfig("resolution"),
				"grid_align":        ArgFixed("true"),
			},
		},
		{
			Engine:    python,
			Command:   path.Join(python, "erodeMask.py"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"file-in":    ArgOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"file-out":   ArgOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"no-data":    ArgFixed("0"),
				"iterations": ArgConfig("coh_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(python, "convert.py"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"file-in":     ArgOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"file-out":    ArgOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"range-in":    ArgFixed("0,1"),
				"dformat-out": ArgConfig("dformat_out"),
			},
		},

		{
			Engine:    python,
			Command:   path.Join(python, "erodeMask.py"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"file-in":    ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"file-out":   ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"no-data":    ArgFixed("0"),
				"iterations": ArgConfig("coh_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(python, "convert.py"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"file-in":     ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"file-out":    ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"range-in":    ArgFixed("0,1"),
				"dformat-out": ArgConfig("dformat_out"),
			},
		},
	}

	return newProcessingGraph(snapPath, steps, infiles, outfiles)
}

// newS1CoregExtractGraph creates a new processing graph to compute Coherence of S1 images
func newS1CoregExtractGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")

	// Define inputs
	infiles := [3][]InFile{
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, pass}},
		{},
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, condDiffT0T2}},
	}

	// Define outputs
	outfiles := [][]OutFile{
		{
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: ToCreate, Condition: condDiffT0T1},
			{File: File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Action: ToDelete, Condition: condDiffT0T1},
		},
		{
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: ToDelete, Condition: condDiffT1T2},
		},
		{},
	}

	// Create processing steps
	steps := []ProcessingStep{
		// Coregistration with ref burst
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_BkG.xml"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"master":         ArgIn{Input: 2, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"slave":          ArgIn{Input: 0, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"output":         ArgOut{service.LayerCoregistrated, service.ExtensionDIMAP},
				"dem_name":       ArgConfig("dem_name"),
				"dem_file":       ArgConfig("dem_file"),
				"dem_nodata":     ArgConfig("dem_nodata"),
				"dem_resampling": ArgConfig("dem_resampling"),
				"resampling":     ArgConfig("bkg_resampling"),
			},
		},

		// Extraction of coregistred slave
		{
			Engine:    snap,
			Command:   path.Join(snap, "S1_SLC_SlvExtract.xml"),
			Condition: condDiffT0T1,

			Args: map[string]Arg{
				"input":  ArgIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"output": ArgOut{service.LayerCoregExtract, service.ExtensionDIMAP},
			},
		},
	}

	return newProcessingGraph(snapPath, steps, infiles, outfiles)
}

// newS1CleanGraph creates a new graph to clean temporary images
func newS1CleanGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")

	// Define inputs
	infiles := [3][]InFile{{}, {}, {}}

	// Define outputs
	outfiles := [][]OutFile{
		{
			{File: File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Action: ToDelete, Condition: pass},
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: ToDelete, Condition: pass},
		},
		{},
		{},
	}

	return newProcessingGraph(snapPath, []ProcessingStep{}, infiles, outfiles)
}

func cmdToString(cmd *exec.Cmd) string {
	s := ""
	for _, a := range cmd.Args {
		s += " " + a
	}
	return s
}

// Process runs the graph
// Returns the files to create or to delete
func (g *ProcessingGraph) Process(ctx context.Context, config GraphConfig, tiles []common.Tile) ([][]OutFile, error) {
	var filter LogFilter
	pythonFilter := PythonLogFilter{}
	snapFilter := SNAPLogFilter{}
	cmdFilter := CmdLogFilter{}
	for _, step := range g.steps {
		if !step.Condition.Pass(tiles) {
			continue
		}

		// Get args list
		args, err := step.formatArgs(config, tiles)
		if err != nil {
			return nil, fmt.Errorf("process.%w", err)
		}

		// Create command
		var cmd *exec.Cmd
		switch step.Engine {
		case snap:
			cmd = exec.Command(g.snap, args...)
			filter = &snapFilter

		case python:
			cmd = exec.Command(step.Command, args...)
			filter = &pythonFilter

		case command:
			cmd = exec.Command(step.Command, args...)
			filter = &cmdFilter
		}

		// Exec graph
		log.Logger(ctx).Sugar().Debug(cmdToString(cmd))
		if err := log.Exec(ctx, cmd, log.StdoutLevel(zapcore.DebugLevel), log.StdoutFilter(filter), log.StderrFilter(filter)); err != nil {
			// Error handling
			if filter != nil {
				err = filter.WrapError(err)
			}
			return nil, fmt.Errorf("process[%s]: %w", cmdToString(cmd), err)
		}
	}

	// OutFiles list
	outfiles := make([][]OutFile, len(tiles))
	for i, outfs := range g.outFiles {
		for _, f := range outfs {
			if f.Condition.Pass(tiles) {
				if err := f.setDFormatOut(config); err != nil {
					return nil, fmt.Errorf("process.%w", err)
				}
				outfiles[i] = append(outfiles[i], f)
			}
		}
	}
	return outfiles, nil
}

type LogFilter interface {
	log.Filter
	// WrapError wraps the error with additionnal information from the logs
	WrapError(err error) error
}

// PythonLogFilter formats log from python
type PythonLogFilter struct {
	lastError string
}

// CmdLogFilter formats log from other commands
type CmdLogFilter struct {
	lastError string
}

// SNAPLogFilter formats log from ESA/SNAP
type SNAPLogFilter struct {
	lastError string
}

var temporaryErrs = []string{
	"temporary failure",
	"timed out",
}

// WrapError implements LogFilter
func (f *PythonLogFilter) WrapError(err error) error {
	if f.lastError != "" {
		err = service.MergeErrors(true, err, fmt.Errorf(f.lastError))
		if err != nil {
			strerr := strings.ToLower(err.Error())
			if strings.Contains(strerr, "FATAL") {
				err = service.MakeFatal(err)
			} else {
				for _, tmpErr := range temporaryErrs {
					if strings.Contains(strerr, tmpErr) {
						return service.MakeTemporary(err)
					}
				}
			}
		}
	}
	return err
}

// Filter implement log.Filter
func (f *PythonLogFilter) Filter(msg string, defaultLevel zapcore.Level) (string, zapcore.Level, bool) {
	trimmedmsg := strings.TrimSpace(msg)
	if strings.HasPrefix(trimmedmsg, "FATAL:") || strings.HasPrefix(trimmedmsg, "ERROR:") {
		f.lastError = msg
		return msg, zapcore.ErrorLevel, false
	}
	return msg, defaultLevel, false
}

// WrapError implements LogFilter
func (f *SNAPLogFilter) WrapError(err error) error {
	if f.lastError != "" && err != nil {
		if strings.Contains(f.lastError, "Try again") || strings.Contains(f.lastError, "Temporary failure") {
			err = service.MakeTemporary(err)
		}
		return fmt.Errorf("%w (%v)", err, f.lastError)
	}
	return err
}

// Filter implement log.Filter
func (f *SNAPLogFilter) Filter(msg string, defaultLevel zapcore.Level) (string, zapcore.Level, bool) {
	trimmedmsg := strings.TrimSpace(msg)

	if strings.HasPrefix(trimmedmsg, "java.") && strings.Contains(msg, "Exception") {
		return msg, zapcore.WarnLevel, false
	}
	if strings.HasPrefix(trimmedmsg, "at ") {
		return msg, zapcore.DebugLevel, false
	}
	if strings.HasPrefix(trimmedmsg, "INFO:") {
		return msg, zapcore.DebugLevel, false
	}
	if strings.HasPrefix(trimmedmsg, "-- org.jblas INFO") {
		return msg, zapcore.DebugLevel, false
	}
	if strings.HasPrefix(trimmedmsg, "SEVERE:") {
		return msg, zapcore.InfoLevel, false
	}
	if strings.HasPrefix(trimmedmsg, "WARNING:") {
		return msg, zapcore.WarnLevel, false
	}
	if strings.HasPrefix(trimmedmsg, "Error:") {
		f.lastError = msg
		return msg, zapcore.ErrorLevel, false
	}
	return msg, defaultLevel, false
}

// WrapError implements LogFilter
func (f *CmdLogFilter) WrapError(err error) error {
	if f.lastError != "" && err != nil {
		if strings.Contains(f.lastError, "FATAL ERROR:") {
			err = service.MakeFatal(err)
		}
		if strings.Contains(f.lastError, "TEMPORARY ERROR:") {
			err = service.MakeTemporary(err)
		}
		return fmt.Errorf("%w (%v)", err, f.lastError)
	}
	return err
}

// Filter implement log.Filter
func (f *CmdLogFilter) Filter(msg string, defaultLevel zapcore.Level) (string, zapcore.Level, bool) {
	msg = strings.TrimSuffix(msg, "\n")
	trimmedmsg := strings.TrimSpace(msg)
	if strings.Contains(trimmedmsg, "ERROR:") {
		f.lastError = msg
		return msg, zapcore.ErrorLevel, false
	} else if strings.HasPrefix(trimmedmsg, "WARN:") {
		return msg, zapcore.WarnLevel, false
	}
	return msg, zapcore.DebugLevel, false
}

func (step ProcessingStep) formatArgs(config GraphConfig, tiles []common.Tile) ([]string, error) {

	var args []string
	switch step.Engine {
	case snap:
		// Graph path and Standard args
		args = []string{step.Command,
			"-x",
			"-q", config["snap_cpu_parallelism"],
			"-J-Xmx9000m",
		}
		// Add args
		for param, arg := range step.Args {
			value, err := formatArgs(arg, config, tiles)
			if err != nil {
				return nil, fmt.Errorf("formatArgs.%w", err)
			}

			// Append arg
			args = append(args, fmt.Sprintf("-P%s=%s", param, value))
		}
	case python, command:
		// Add args
		for param, arg := range step.Args {
			value, err := formatArgs(arg, config, tiles)
			if err != nil {
				return nil, fmt.Errorf("formatArgs.%w", err)
			}

			// Append arg
			args = append(args, fmt.Sprintf("--%s=%s", param, value))
		}
	}

	return args, nil
}

func formatArgs(arg Arg, config GraphConfig, tiles []common.Tile) (string, error) {
	var valstr string
	switch key := arg.(type) {
	// Input (tile)
	case ArgIn:
		if key.Input >= len(tiles) {
			return "", fmt.Errorf("ArgIn: not enough tiles provided")
		}
		valstr = service.LayerFileName(tiles[key.Input], key.Layer, key.Extension)

	// Output
	case ArgOut:
		valstr = service.LayerFileName(tiles[0], service.Layer(key.Layer), key.Extension)

	// Fixed arg
	case ArgFixed:
		valstr = string(key)

	// Specific args from tile
	case ArgTile:
		switch key {
		case burstSwath:
			valstr = tiles[0].Data.SwathID
		case sceneDate:
			valstr = tiles[0].Scene.Data.Date.Format(snapDateFormat)
		case burstCohDate:
			if tiles[1].Scene.SourceID == tiles[2].Scene.SourceID {
				valstr = tiles[0].Scene.Data.Date.Format(snapDateFormat)
			} else {
				valstr = tiles[2].Scene.Data.Date.Format(snapDateFormat)
			}
		case tileNumber:
			valstr = fmt.Sprintf("%d", tiles[0].Data.TileNr)
		case sceneName:
			valstr = tiles[0].Scene.SourceID
		default:
			return "", fmt.Errorf("key '%s' not found in tile", key)
		}

	// Specific args from config
	case ArgConfig:
		var ok bool
		if valstr, ok = config[string(key)]; !ok {
			return "", fmt.Errorf("key '%s' not found in config", key)
		}

	default:
		return "", fmt.Errorf("unknow Arg Type: %v", key)
	}

	return valstr, nil
}
