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

// ProcessTile processes a tile.
func ProcessTile(ctx context.Context, storageService service.Storage, gcclient *geocube.Client, tile common.TileToProcess, workdir string) error {
	// Working dir
	workdir = filepath.Join(workdir, uuid.New().String())

	if err := os.MkdirAll(workdir, 0766); err != nil {
		return service.MakeTemporary(fmt.Errorf("make directory %s: %w", workdir, err))
	}
	defer os.RemoveAll(workdir)
	if err := os.Chdir(workdir); err != nil {
		return service.MakeTemporary(fmt.Errorf("chdir: %w", err))
	}

	// Graph
	g, config, err := graph.LoadGraph(ctx, tile.Data.GraphName)
	if err != nil {
		return fmt.Errorf("ProcessTile[%s_%s].%w", tile.Scene.Data.Date.Format("20060102"), tile.SourceID, err)
	}
	// Append the user config
	for key, val := range tile.Scene.Data.GraphConfig {
		config[key] = val
	}

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
	log.Logger(ctx).Sugar().Infof("import tile %s_%s", tile.Scene.Data.Date.Format("20060102"), tile.SourceID)
	imported := service.StringSet{}
	for i, infiles := range g.InFiles {
		for _, infile := range infiles {
			// Do not import twice
			filename := service.LayerFileName(tiles[i], infile.Layer, infile.Extension)
			if !imported.Exists(filename) && infile.Condition.Pass(tiles) {
				log.Logger(ctx).Sugar().Debugf("import layer %s_%s_%s", tiles[i].Scene.Data.Date.Format("20060102"), tiles[i].SourceID, infile.Layer)
				imported.Push(filename)
				if err := storageService.ImportLayer(ctx, tiles[i], infile.Layer, infile.Extension, workdir); err != nil {
					return fmt.Errorf("ProcessTile[%s_%s].%w", tile.Scene.Data.Date.Format("20060102"), tile.SourceID, err)
				}
			}
		}
	}

	// Process graph
	log.Logger(ctx).Sugar().Infof("process tile %s with graph %s", tile.SourceID, tile.Data.GraphName)
	outfiles, err := g.Process(ctx, config, tiles)
	if err != nil {
		return fmt.Errorf("ProcessTile[%s_%s].%w", tile.Scene.Data.Date.Format("20060102"), tile.SourceID, err)
	}

	indexed := false
	for i, outtilefile := range outfiles {
		logtilename := fmt.Sprintf("%s_%s", tiles[i].Scene.Data.Date.Format("20060102"), tiles[i].SourceID)
		for _, f := range outtilefile {
			switch f.Action {
			case graph.ToCreate, graph.ToIndex:
				// Export output layers to storage
				log.Logger(ctx).Sugar().Infof("save tile %s.%s", logtilename, f.Layer)
				uri, err := storageService.SaveLayer(ctx, tiles[i], f.Layer, f.Extension, workdir)
				if err != nil {
					return fmt.Errorf("ProcessTile[%s].%w", logtilename, err)
				}
				// Index tile
				if f.Action == graph.ToIndex {
					log.Logger(ctx).Sugar().Infof("index tile %s.%s", logtilename, f.Layer)
					if err := indexTile(ctx, gcclient, tiles[i], tiles[i].Scene.Data.InstancesID, tiles[i].Scene.Data.RecordID, f, uri); err != nil {
						if geocube.Code(err) == codes.AlreadyExists {
							log.Logger(ctx).Sugar().Warnf("ProcessTile[%s].indexTile already exists", logtilename)
						} else {
							return fmt.Errorf("ProcessTile[%s].indexTile.%w", logtilename, err)
						}
					}
					indexed = true
				}
			case graph.ToDelete:
				log.Logger(ctx).Sugar().Infof("delete tile %s.%s", logtilename, f.Layer)
				if err := storageService.DeleteLayer(ctx, tiles[i], f.Layer, f.Extension); err != nil && !errors.As(err, &service.ErrFileNotFound{}) {
					return err
				}
			}
		}
	}

	if indexed {
		// Update record processing date (errors are not fatal)
		if _, err := gcclient.AddRecordsTags([]string{tile.Scene.Data.RecordID}, map[string]string{common.TagProcessingDate: time.Now().Format("2006-01-02 15:04:05")}); err != nil {
			log.Logger(ctx).Sugar().Warnf("UpdateRecordTag[%s] fails: %v", tile.Scene.Data.RecordID, err)
		}
	}

	return nil
}

// indexTile indexes the tile in the Geocube
func indexTile(ctx context.Context, gcclient *geocube.Client, tile common.Tile, instancesID map[string]string, recordID string, file graph.OutFile, uri string) error {
	// Get dataset information
	dformat := geocubepb.DataFormat{
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

	// Index
	if err := gcclient.IndexDataset(uri, true, "", recordID, instanceID, []int64{1}, &dformat, file.ExtMin, file.ExtMax, file.Exponent); err != nil {
		return err
	}
	return nil
}
