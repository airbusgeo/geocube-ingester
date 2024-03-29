package downloader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/graph"
	"github.com/airbusgeo/geocube-ingester/interface/provider"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/google/uuid"
)

// ProcessScene processes a scene.
func ProcessScene(ctx context.Context, imageProviders []provider.ImageProvider, storageService service.Storage, scene common.Scene, workdir string, opts []graph.Option) error {
	// Working dir
	workdir = filepath.Join(workdir, uuid.New().String())

	if err := os.MkdirAll(workdir, 0766); err != nil {
		return service.MakeTemporary(fmt.Errorf("make directory %s: %w", workdir, err))
	}
	defer os.RemoveAll(workdir)
	if err := os.Chdir(workdir); err != nil {
		return service.MakeTemporary(fmt.Errorf("chdir: %w", err))
	}

	// Download with the first successful imageProvider
	log.Logger(ctx).Sugar().Infof("downloading %s", scene.SourceID)
	var err error
	for _, imageProvider := range imageProviders {
		e := imageProvider.Download(ctx, scene, workdir)
		if err = service.MergeErrors(false, err, e); err == nil {
			log.Logger(ctx).Sugar().Infof("%s downloaded from %s", scene.SourceID, imageProvider.Name())
			break
		}
		log.Logger(ctx).Sugar().Warnf("%v", e)
	}
	if err != nil {
		return fmt.Errorf("ProcessScene.ImageProviders.%w", err)
	}

	log.Logger(ctx).Sugar().Infof("processing %s with %s", scene.SourceID, scene.Data.GraphName)
	for sourceID := range scene.Data.TileMappings {
		err := ProcessTile(ctx, storageService, scene, sourceID, workdir, opts)
		if err != nil {
			return fmt.Errorf("ProcessScene.%w", err)
		}
	}

	return nil
}

// ProcessTile extracts the tile from the scene and preprocesses it
func ProcessTile(ctx context.Context, storageService service.Storage, scene common.Scene, tile, workdir string, opts []graph.Option) error {
	ctx = log.With(ctx, "tile", tile)

	if scene.Data.IsRetriable {
		opts = append(opts, graph.WithRetriableErrors())
	}

	// Load the graph
	g, config, envs, err := graph.LoadGraph(ctx, scene.Data.GraphName, opts...)
	if err != nil {
		return err
	}
	// Append the user config
	for key, val := range scene.Data.GraphConfig {
		config[key] = val
	}

	// Append workdir
	config["workdir"] = workdir

	// Input tile
	mapping := scene.Data.TileMappings[tile]
	tiles := []common.Tile{
		{
			SourceID: tile,
			Scene:    scene,
			Data: common.TileAttrs{
				SwathID: mapping.SwathID,
				TileNr:  mapping.TileNr,
			},
		},
	}

	// Process graph
	outfiles, err := g.Process(ctx, config, envs, tiles)
	if err != nil {
		return fmt.Errorf("ProcessTile[%s].%w", tile, err)
	}

	// Export output layers to storage
	for i, outtilefiles := range outfiles {
		logtilename := fmt.Sprintf("%s_%s", tiles[i].Scene.Data.Date.Format("20060102"), tiles[i].SourceID)
		for _, f := range outtilefiles {
			switch f.Action {
			case graph.ToCreate:
				dst, err := storageService.SaveLayer(ctx, tiles[i], f.Layer, f.Extension, workdir)
				if err != nil {
					return fmt.Errorf("ProcessTile[%s].%w", logtilename, err)
				}
				log.Logger(ctx).Sugar().Debugf("%s exported to %s", logtilename, dst)
			}
		}
	}

	return nil
}
