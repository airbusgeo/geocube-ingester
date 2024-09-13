package processor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	geocube "github.com/airbusgeo/geocube-client-go/client"
	geocubepb "github.com/airbusgeo/geocube-client-go/pb"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/graph"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
)

type outFileTile struct {
	file graph.OutFile
	tile common.Tile
}

// ProcessTile processes a tile.
func ProcessTile(ctx context.Context, storageService service.Storage, gcclient *geocube.Client, tile common.TileToProcess, workdir string, opts []graph.Option) error {
	tag := fmt.Sprintf("%s_%s", tile.Scene.Data.Date.Format("20060102"), tile.SourceID)

	// Working dir
	workdir = filepath.Join(workdir, uuid.New().String())

	if err := os.MkdirAll(workdir, 0766); err != nil {
		return service.MakeTemporary(fmt.Errorf("make directory %s: %w", workdir, err))
	}
	defer os.RemoveAll(workdir)
	if err := os.Chdir(workdir); err != nil {
		return service.MakeTemporary(fmt.Errorf("chdir: %w", err))
	}

	if tile.Data.IsRetriable {
		opts = append(opts, graph.WithRetriableErrors())
	}

	// Graph
	g, config, envs, err := graph.LoadGraph(ctx, tile.Data.GraphName, opts...)
	if err != nil {
		return fmt.Errorf("ProcessTile.%w", err)
	}
	// Append the user config
	for key, val := range tile.Scene.Data.GraphConfig {
		config[key] = val
	}

	// Append workdir
	config["workdir"] = workdir

	// Input tiles
	tiles := []common.Tile{tile.Tile}
	if tile.Previous.SourceID == "" {
		tiles = append(tiles, tiles[0])
	} else {
		tiles = append(tiles, tile.Previous)
	}
	if tile.Reference.SourceID == "" {
		tiles = append(tiles, tiles[0])
	} else {
		tiles = append(tiles, tile.Reference)
	}

	// Import input layers from storage
	log.Logger(ctx).Info("import tile")
	imported := service.StringSet{}
	for i, infiles := range g.InFiles {
		for _, infile := range infiles {
			// Do not import twice
			filename := service.LayerFileName(tiles[i], infile.Layer, infile.Extension)
			if fn, ok := infile.Condition.PassFn.(graph.TileConditionFn); ok && !imported.Exists(filename) && fn(tiles) {
				log.Logger(ctx).Sugar().Debugf("import layer '%s'", infile.Layer)
				imported.Push(filename)
				if err := storageService.ImportLayer(ctx, tiles[i], infile.Layer, infile.Extension, workdir); err != nil {
					return fmt.Errorf("ProcessTile[%s].%w", tag, err)
				}
			}
		}
	}

	// Process graph
	log.Logger(ctx).Sugar().Infof("process with graph '%s'", tile.Data.GraphName)
	outfiles, processErr := g.Process(ctx, config, envs, tiles)

	// Handle outFiles
	outFileErr := func() error {
		toIndex := map[string]outFileTile{}
		var toDelete []outFileTile
		// Create file, delay indexation and deletion
		for i, outtilefile := range outfiles {
			for _, f := range outtilefile {
				switch f.Action {
				case graph.ToCreate, graph.ToIndex:
					// Export output layers to storage
					log.Logger(ctx).Sugar().Infof("save layer '%s'", f.Layer)
					uri, err := storageService.SaveLayer(ctx, tiles[i], f.Layer, f.Extension, workdir)
					if err != nil {
						return fmt.Errorf("ProcessTile[%s].%w", tag, err)
					}
					// Index tile => differ
					if f.Action == graph.ToIndex {
						toIndex[uri] = outFileTile{file: f, tile: tiles[i]}
					}
				case graph.ToDelete:
					toDelete = append(toDelete, outFileTile{file: f, tile: tiles[i]})
				}
			}
		}

		// Index tiles
		if err := service.Retriable(ctx, func() error {
			for uri, f := range toIndex {
				log.Logger(ctx).Sugar().Infof("index layer %s", f.file.Layer)
				if err := indexTile(ctx, gcclient, f.tile.Scene.Data.InstancesID, f.tile.Scene.Data.RecordID, f.file, uri); err != nil {
					if geocube.Code(err) == codes.AlreadyExists {
						log.Logger(ctx).Sugar().Warnf("layer %s already exists: %v", f.file.Layer, err)
					} else {
						return err
					}
				}
			}
			return nil
		}, 15*time.Second, 3); err != nil {
			return fmt.Errorf("ProcessTile[%s].%w (after 3 retries)", tag, err)
		}

		// Delete tiles at the end to ease a retry
		for _, f := range toDelete {
			log.Logger(ctx).Sugar().Infof("delete layer '%s'", f.file.Layer)
			if err := storageService.DeleteLayer(ctx, f.tile, f.file.Layer, f.file.Extension); err != nil && !errors.As(err, &service.ErrFileNotFound{}) {
				return fmt.Errorf("ProcessTile[%s].%w", tag, err)
			}
		}

		if len(toIndex) > 0 {
			// Update record processing date (errors are not fatal)
			if _, err := gcclient.AddRecordsTags(ctx, []string{tile.Scene.Data.RecordID}, map[string]string{common.TagProcessingDate: time.Now().Format("2006-01-02 15:04:05")}); err != nil {
				log.Logger(ctx).Sugar().Warnf("UpdateRecordTag[%s] fails: %v", tile.Scene.Data.RecordID, err)
			}
		}
		return nil
	}()

	if processErr != nil {
		if outFileErr != nil {
			return fmt.Errorf("%w (during cleaning, an other error occured: %v)", processErr, outFileErr)
		}
		return processErr
	}

	return outFileErr
}

// indexTile indexes the tile in the Geocube
func indexTile(ctx context.Context, gcclient *geocube.Client, instancesID map[string]string, recordID string, file graph.OutFile, uri string) error {
	// Get dataset information
	dformat := geocube.DataFormat{
		NoData:   file.NoData,
		MinValue: file.Min,
		MaxValue: file.Max,
	}

	switch file.DType {
	default:
		return fmt.Errorf("indexTile: file.DType '%v' not found", file.DType)
	case graph.UInt8:
		dformat.Dtype = geocubepb.DataFormat_UInt8
	case graph.UInt16:
		dformat.Dtype = geocubepb.DataFormat_UInt16
	case graph.UInt32:
		dformat.Dtype = geocubepb.DataFormat_UInt32
	case graph.Int16:
		dformat.Dtype = geocubepb.DataFormat_Int16
	case graph.Int32:
		dformat.Dtype = geocubepb.DataFormat_Int32
	case graph.Float32:
		dformat.Dtype = geocubepb.DataFormat_Float32
	case graph.Float64:
		dformat.Dtype = geocubepb.DataFormat_Float64
	case graph.Complex64:
		dformat.Dtype = geocubepb.DataFormat_Complex64
	}

	// Get instance ID
	instanceID, ok := instancesID[string(file.Layer)]
	if !ok {
		return fmt.Errorf("indexTile: layer %s not found in InstancesID", file.Layer)
	}

	var bands []int64
	for band := 1; band <= file.Nbands; band++ {
		bands = append(bands, int64(band))
	}

	// Index
	if err := gcclient.IndexDataset(ctx, uri, true, "", recordID, instanceID, bands, &dformat, file.ExtMin, file.ExtMax, file.Exponent); err != nil {
		return err
	}
	return nil
}
