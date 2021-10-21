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

//ProcessScene processes a scene.
func ProcessScene(ctx context.Context, imageProviders []provider.ImageProvider, storageService service.Storage, scene common.Scene, workdir string) error {
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
		e := imageProvider.Download(ctx, scene.SourceID, scene.Data.UUID, workdir)
		if err = service.MergeErrors(false, err, e); err == nil {
			break
		}
		log.Logger(ctx).Sugar().Warnf("%v", e)
	}
	if err != nil {
		return fmt.Errorf("ProcessScene.ImageProviders.%w", err)
	}

	log.Logger(ctx).Sugar().Infof("processing %s", scene.SourceID)
	for sourceID := range scene.Data.TileMappings {
		err := ProcessTile(ctx, storageService, scene, sourceID)
		if err != nil {
			return fmt.Errorf("ProcessScene.%w", err)
		}
	}

	return nil
}

// ProcessTile extracts the tile from the scene and preprocesses it
func ProcessTile(ctx context.Context, storageService service.Storage, scene common.Scene, tile string) error {
	// Load the graph
	g, config, err := graph.LoadGraph(scene.Data.GraphName)
	if err != nil {
		return err
	}
	// Append the user config
	for key, val := range scene.Data.GraphConfig {
		config[key] = val
	}

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
	outfiles, err := g.Process(ctx, config, tiles)
	if err != nil {
		return fmt.Errorf("ProcessTile[%s].%w", tile, err)
	}

	// Export output layers to storage
	for i, outtilefiles := range outfiles {
		logtilename := fmt.Sprintf("%s_%s", tiles[i].Scene.Data.Date.Format("20060102"), tiles[i].SourceID)
		for _, f := range outtilefiles {
			switch f.Status {
			case graph.ToCreate:
				if _, err := storageService.SaveLayer(ctx, tiles[i], f.Layer, f.Extension, ""); err != nil {
					return fmt.Errorf("ProcessTile[%s].%w", logtilename, err)
				}
			}
		}
	}

	return nil
}
