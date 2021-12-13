package graph

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
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

func condDiffB0B1(tiles []common.Tile) bool {
	return tiles[0].Scene.SourceID != tiles[1].Scene.SourceID
}

func condDiffB1B2(tiles []common.Tile) bool {
	return tiles[1].Scene.SourceID != tiles[2].Scene.SourceID
}

type arg interface{}

type argTileIn struct { // argTileIn : tile input of the graph
	Input     int // Num of input [0, 1, 2]
	Layer     service.Layer
	Extension service.Extension
}
type argOut struct { // output of the graph
	service.Layer
	Extension service.Extension
}
type argFixed string  // fixed arg
type argConfig string // arg from config
type argTile string   // arg from tile info

// tileCondition is a condition on tiles to execute a step or to create an outfile
type tileCondition func([]common.Tile) bool

// pass is a tileCondition always true
var pass tileCondition = func([]common.Tile) bool { return true }

type processingStep struct {
	Engine    string // Python or Snap
	Command   string
	Args      map[string]arg
	Condition tileCondition
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

// OutFileStatus
type OutFileStatus int32

// OutFileStatus
const (
	ToIgnore OutFileStatus = iota
	ToCreate
	ToIndex
	ToDelete
)

// File is a layer with an extension
type File struct {
	Layer     service.Layer
	Extension service.Extension
}

// InFile describes an input file of the processing
type InFile struct {
	File
	Optional bool
}

// OutFile describes an output file of the processing
type OutFile struct {
	File
	dformatOut       arg // argFixed or argConfig
	DType            DType
	NoData           float64
	Min, Max         float64
	RealMin, RealMax float64
	Exponent         float64
	Status           OutFileStatus
}

func newOutFile(layer service.Layer, ext service.Extension, dformatOut arg, realmin, realmax, exponent float64, status OutFileStatus) OutFile {
	return OutFile{
		File: File{
			Layer:     layer,
			Extension: ext,
		},
		dformatOut: dformatOut,
		RealMin:    realmin,
		RealMax:    realmax,
		Exponent:   exponent,
		Status:     status,
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
	steps        []processingStep
	snap         string
	InFiles      [3][]InFile
	outFiles     [][]OutFile
	outfilesCond [][]tileCondition // Conditions to output a file (must have the same dimension as outFiles)
}

func newProcessingGraph(snapPath string, steps []processingStep, infiles [3][]InFile, outfiles [][]OutFile, outfilesCond [][]tileCondition) (*ProcessingGraph, error) {
	// Check commands
	snapRequired := true
	for _, step := range steps {
		if _, err := os.Stat(step.Command); err != nil {
			return nil, fmt.Errorf("newProcessingGraph: Command not found: %s", step.Command)
		}
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
		snap:         snapPath,
		steps:        steps,
		InFiles:      infiles,
		outFiles:     outfiles,
		outfilesCond: outfilesCond,
	}, nil
}

// LoadGraph returns the graph from its name and its default configuration
func LoadGraph(graphName string) (*ProcessingGraph, GraphConfig, error) {
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
	return nil, nil, fmt.Errorf("unknown graph: %s", graphName)
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
		"resolution":                 "20",
		"projection":                 "EPSG:4326",
		"bs_erode_iterations":        "10",
		"coh_erode_iterations":       "10",
		"dformat-out":                "float32,0,0,1", // option to map float32[0,1] to another lighter format (ie. int16,-32768,0,1000)
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
	graphPath := Getenv("GRAPHPATH", "/data/graph")

	// Define inputs
	infiles := [3][]InFile{}

	// Define outputs
	outfiles := [][]OutFile{
		{newOutFile(service.LayerPreprocessed, service.ExtensionDIMAP, argFixed("float32,0,0,1"), 0, 1, 1, ToCreate)},
		{},
		{},
	}

	outfilesCond := [][]tileCondition{{pass}, {}, {}}

	// Create processing steps
	steps := []processingStep{
		// Extract burst from image and preprocess
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_BurstSplit_AO_CAL.xml"),
			Condition: pass,

			Args: map[string]arg{
				"input":  argTile(sceneName),
				"output": argOut{service.LayerPreprocessed, service.ExtensionDIMAP},
				"swath":  argTile(burstSwath),
				"polar":  argFixed("\"VV VH\""),
				"burst":  argTile(tileNumber),
			},
		},
	}

	return newProcessingGraph(snapPath, steps, infiles, outfiles, outfilesCond)
}

// newS1BsCohGraph creates a new processing graph to compute Backscatter and Coherence of S1 images
func newS1BsCohGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")
	graphPath := Getenv("GRAPHPATH", "/data/graph")

	// Define inputs
	infiles := [3][]InFile{
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, false}},
		{{File{service.LayerCoregExtract, service.ExtensionDIMAP}, true}},
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, false}},
	}

	// Define outputs
	outfiles := [][]OutFile{
		{
			newOutFile(service.LayerBackscatterVV, service.ExtensionGTiff, argConfig("dformat-out"), 0, 1, 1, ToIndex),
			newOutFile(service.LayerBackscatterVH, service.ExtensionGTiff, argConfig("dformat-out"), 0, 1, 1, ToIndex),
			newOutFile(service.LayerCoherenceVV, service.ExtensionGTiff, argConfig("dformat-out"), 0, 1, 1, ToIndex),
			newOutFile(service.LayerCoherenceVH, service.ExtensionGTiff, argConfig("dformat-out"), 0, 1, 1, ToIndex),
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Status: ToCreate},
			{File: File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Status: ToDelete},
		},
		{
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Status: ToDelete},
		},
		{},
	}

	outfilesCond := [][]tileCondition{
		{pass, pass, condDiffB0B1, condDiffB0B1, condDiffB0B1, condDiffB0B1},
		{condDiffB1B2},
		{},
	}

	// Create processing steps
	steps := []processingStep{
		// Coregistration with ref burst
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_BkG.xml"),
			Condition: pass,

			Args: map[string]arg{
				"master":         argTileIn{Input: 2, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"slave":          argTileIn{Input: 0, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"output":         argOut{service.LayerCoregistrated, service.ExtensionDIMAP},
				"dem_name":       argConfig("dem_name"),
				"dem_file":       argConfig("dem_file"),
				"dem_nodata":     argConfig("dem_nodata"),
				"dem_resampling": argConfig("dem_resampling"),
				"output_deramp":  argFixed("true"),
			},
		},

		// Extraction of coregistred slave
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_SlvExtract.xml"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"input":  argTileIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"output": argOut{service.LayerCoregExtract, service.ExtensionDIMAP},
			},
		},

		// Backscatter computation
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_Deb_BetaSigma_ML_TC_RNKELL.xml"),
			Condition: pass,

			Args: map[string]arg{
				"input":             argTileIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"outputVV":          argOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"outputVH":          argOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"range_multilook":   argConfig("terrain_correction_range"),
				"azimuth_multilook": argConfig("terrain_correction_azimuth"),
				"dem_name":          argConfig("dem_name"),
				"dem_file":          argConfig("dem_file"),
				"dem_nodata":        argConfig("dem_nodata"),
				"dem_egm":           argConfig("dem_egm_correction"),
				"dem_resampling":    argConfig("dem_resampling"),
				"img_resampling":    argConfig("img_resampling"),
				"projection":        argConfig("projection"),
				"resolution":        argConfig("resolution"),
				"grid_align":        argFixed("true"),
				"band":              argFixed("Sigma0"),
				"trig":              argFixed("sin"),
				"swath":             argTile(burstSwath),
				"img_suffix":        argTile(sceneDate),
			},
		},

		{
			Engine:    python,
			Command:   path.Join(graphPath, python, "erodeMask.py"),
			Condition: pass,

			Args: map[string]arg{
				"file-in":    argOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"file-out":   argOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"no-data":    argFixed("0"),
				"iterations": argConfig("bs_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(graphPath, python, "convert.py"),
			Condition: pass,

			Args: map[string]arg{
				"file-in":     argOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"file-out":    argOut{service.LayerBackscatterVV, service.ExtensionGTiff},
				"range-in":    argFixed("0,1"),
				"dformat-out": argConfig("dformat-out"),
			},
		},

		{
			Engine:    python,
			Command:   path.Join(graphPath, python, "erodeMask.py"),
			Condition: pass,

			Args: map[string]arg{
				"file-in":    argOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"file-out":   argOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"no-data":    argFixed("0"),
				"iterations": argConfig("bs_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(graphPath, python, "convert.py"),
			Condition: pass,

			Args: map[string]arg{
				"file-in":     argOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"file-out":    argOut{service.LayerBackscatterVH, service.ExtensionGTiff},
				"range-in":    argFixed("0,1"),
				"dformat-out": argConfig("dformat-out"),
			},
		},

		// Coregistration with prev burst
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_BkG.xml"),
			Condition: condDiffB1B2,

			Args: map[string]arg{
				"master":         argTileIn{Input: 0, Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP},
				"slave":          argTileIn{Input: 1, Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP},
				"output":         argOut{service.LayerCoregistrated, service.ExtensionDIMAP},
				"dem_name":       argConfig("dem_name"),
				"dem_file":       argConfig("dem_file"),
				"dem_nodata":     argConfig("dem_nodata"),
				"dem_resampling": argConfig("dem_resampling"),
				"output_deramp":  argFixed("false"),
			},
		},

		// Coherence computation
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_Coh_BSel_Deb_ML_TC.xml"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"input":             argTileIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"outputVV":          argOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"outputVH":          argOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"coherence_range":   argConfig("coherence_range"),
				"coherence_azimuth": argConfig("coherence_azimuth"),
				"sel_date":          argTile(burstCohDate),
				"range_multilook":   argConfig("terrain_correction_range"),
				"azimuth_multilook": argConfig("terrain_correction_azimuth"),
				"dem_name":          argConfig("dem_name"),
				"dem_file":          argConfig("dem_file"),
				"dem_nodata":        argConfig("dem_nodata"),
				"dem_egm":           argConfig("dem_egm_correction"),
				"dem_resampling":    argConfig("dem_resampling"),
				"img_resampling":    argConfig("img_resampling"),
				"projection":        argConfig("projection"),
				"resolution":        argConfig("resolution"),
				"grid_align":        argFixed("true"),
			},
		},
		{
			Engine:    python,
			Command:   path.Join(graphPath, python, "erodeMask.py"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"file-in":    argOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"file-out":   argOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"no-data":    argFixed("0"),
				"iterations": argConfig("coh_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(graphPath, python, "convert.py"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"file-in":     argOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"file-out":    argOut{service.LayerCoherenceVV, service.ExtensionGTiff},
				"range-in":    argFixed("0,1"),
				"dformat-out": argConfig("dformat-out"),
			},
		},

		{
			Engine:    python,
			Command:   path.Join(graphPath, python, "erodeMask.py"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"file-in":    argOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"file-out":   argOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"no-data":    argFixed("0"),
				"iterations": argConfig("coh_erode_iterations"),
			},
		},

		{
			Engine:    command,
			Command:   path.Join(graphPath, python, "convert.py"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"file-in":     argOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"file-out":    argOut{service.LayerCoherenceVH, service.ExtensionGTiff},
				"range-in":    argFixed("0,1"),
				"dformat-out": argConfig("dformat-out"),
			},
		},
	}

	return newProcessingGraph(snapPath, steps, infiles, outfiles, outfilesCond)
}

// newS1CoregExtractGraph creates a new processing graph to compute Coherence of S1 images
func newS1CoregExtractGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")
	graphPath := Getenv("GRAPHPATH", "/data/graph")

	// Define inputs
	infiles := [3][]InFile{
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, false}},
		{},
		{{File{service.LayerPreprocessed, service.ExtensionDIMAP}, false}},
	}

	// Define outputs
	outfiles := [][]OutFile{
		{
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Status: ToCreate},
			{File: File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Status: ToDelete},
		},
		{
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Status: ToDelete},
		},
		{},
	}

	outfilesCond := [][]tileCondition{
		{condDiffB0B1, condDiffB0B1},
		{condDiffB1B2},
		{},
	}

	// Create processing steps
	steps := []processingStep{
		// Coregistration with ref burst
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_BkG.xml"),
			Condition: pass,

			Args: map[string]arg{
				"master":         argTileIn{Input: 2, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"slave":          argTileIn{Input: 0, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP},
				"output":         argOut{service.LayerCoregistrated, service.ExtensionDIMAP},
				"dem_name":       argConfig("dem_name"),
				"dem_file":       argConfig("dem_file"),
				"dem_nodata":     argConfig("dem_nodata"),
				"dem_resampling": argConfig("dem_resampling"),
				"output_deramp":  argFixed("true"),
			},
		},

		// Extraction of coregistred slave
		{
			Engine:    snap,
			Command:   path.Join(graphPath, snap, "S1_SLC_SlvExtract.xml"),
			Condition: condDiffB0B1,

			Args: map[string]arg{
				"input":  argTileIn{Input: 0, Layer: service.LayerCoregistrated, Extension: service.ExtensionDIMAP},
				"output": argOut{service.LayerCoregExtract, service.ExtensionDIMAP},
			},
		},
	}

	return newProcessingGraph(snapPath, steps, infiles, outfiles, outfilesCond)
}

// newS1CleanGraph creates a new graph to clean temporary images
func newS1CleanGraph() (*ProcessingGraph, error) {
	snapPath := Getenv("SNAPPATH", "/usr/local/snap/bin/gpt")

	// Define inputs
	infiles := [3][]InFile{{}, {}, {}}

	// Define outputs
	outfiles := [][]OutFile{
		{
			{File: File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Status: ToDelete},
			{File: File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Status: ToDelete},
		},
		{},
		{},
	}

	outfilesCond := [][]tileCondition{
		{pass, pass},
		{},
		{},
	}

	return newProcessingGraph(snapPath, []processingStep{}, infiles, outfiles, outfilesCond)
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
	for _, step := range g.steps {
		if !step.Condition(tiles) {
			continue
		}

		// Get args list
		args, err := step.formatArgs(config, tiles)
		if err != nil {
			return nil, fmt.Errorf("Process.%w", err)
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
			filter = nil
		}

		// Exec graph
		log.Logger(ctx).Sugar().Debug(cmdToString(cmd))
		if err := log.Exec(ctx, cmd, log.StdoutLevel(zapcore.DebugLevel), log.StdoutFilter(filter), log.StderrFilter(filter)); err != nil {
			// Error handling
			if filter != nil {
				err = filter.WrapError(err)
			}
			return nil, fmt.Errorf("Process: %w", err)
		}
	}

	// OutFiles list
	outfiles := make([][]OutFile, len(tiles))
	for i, outfs := range g.outFiles {
		for j, f := range outfs {
			if g.outfilesCond[i][j](tiles) {
				if err := f.setDFormatOut(config); err != nil {
					return nil, fmt.Errorf("Process: %w", err)
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

// SNAPLogFilter formats log from ESA/SNAP
type SNAPLogFilter struct {
	lastError string
}

// WrapError implements LogFilter
func (f *PythonLogFilter) WrapError(err error) error {
	if f.lastError != "" && err != nil {
		if strings.Contains(f.lastError, "FATAL") {
			err = service.MakeFatal(err)
		}
		return fmt.Errorf("%w (%v)", err, f.lastError)
	}
	return err
}

// Filter implement log.Filter
func (f *PythonLogFilter) Filter(msg string, defaultLevel zapcore.Level) (string, zapcore.Level, bool) {
	trimmedmsg := strings.TrimSpace(msg)
	if strings.HasPrefix(trimmedmsg, "FATAL:") {
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

func (step processingStep) formatArgs(config GraphConfig, tiles []common.Tile) ([]string, error) {

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

func formatArgs(arg arg, config GraphConfig, tiles []common.Tile) (string, error) {
	var valstr string
	switch key := arg.(type) {
	// Input (tile)
	case argTileIn:
		if key.Input >= len(tiles) {
			return "", fmt.Errorf("argTileIn: not enough tiles provided")
		}
		valstr = service.LayerFileName(tiles[key.Input], key.Layer, key.Extension)

	// Output
	case argOut:
		valstr = service.LayerFileName(tiles[0], service.Layer(key.Layer), key.Extension)

	// Fixed arg
	case argFixed:
		valstr = string(key)

	// Specific args from tile
	case argTile:
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
	case argConfig:
		var ok bool
		if valstr, ok = config[string(key)]; !ok {
			return "", fmt.Errorf("key '%s' not found in config", key)
		}

	default:
		return "", fmt.Errorf("unknow Arg Type: %v", key)
	}

	return valstr, nil
}
