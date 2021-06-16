package catalog

import (
	"context"
	"fmt"
	"regexp"
	"runtime"

	geocube "github.com/airbusgeo/geocube-client-go/client"
	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

// Catalog is the main class of this package
type Catalog struct {
	GeocubeClient  *geocube.Client
	WorkflowServer string
	ScihubUser     string
	ScihubPword    string
	GCStorageURL   string
	WorkingDir     string
}

func (c *Catalog) ValidateArea(area *entities.AreaToIngest) error {
	// Check AOI ID
	matched, err := regexp.MatchString("^[a-zA-Z0-9-:_]+([a-zA-Z0-9-:_]+)*$", area.AOIID)
	if err != nil {
		return fmt.Errorf("validateArea.AOI: %w", err)
	}
	if !matched {
		return fmt.Errorf("validateArea: wrong format for AOI (must be chars, numbers and -:_): %w", err)
	}

	// Check constellation
	if entities.GetConstellation(area.SceneType.Constellation) == entities.UndefinedConstellation {
		return fmt.Errorf("validateArea:unrecognized constellation: %s", area.SceneType.Constellation)
	}

	// Check that instances exist
	for k, layer := range area.Layers {
		if layer.InstanceID != "" {
			if _, err := c.GeocubeClient.GetVariableFromInstanceID(layer.InstanceID); err != nil {
				return fmt.Errorf("validateArea: %w", err)
			}
		} else {
			v, err := c.GeocubeClient.GetVariableFromName(layer.Variable)
			if err != nil {
				return fmt.Errorf("validateArea: %w", err)
			}
			vi := v.Instance(layer.Instance)
			if vi == nil {
				return fmt.Errorf("validateArea: unknown instance %s", layer.Instance)
			}
			layer.InstanceID = vi.InstanceID
			area.Layers[k] = layer
		}
	}
	return nil
}

// DoScenesInventory lists scenes for a given AOI, satellites and interval of time
func (c *Catalog) DoScenesInventory(ctx context.Context, area entities.AreaToIngest) ([]*entities.Scene, error) {
	// geos AOI
	aoi, err := geos.FromWKT(wkt.MustEncode(area.AOI.Geometry))
	if err != nil {
		return nil, fmt.Errorf("DoScenesInventory.FromWKT: %w", err)
	}

	// Search scenes covering this area
	log.Logger(ctx).Sugar().Debugf("Search scenes for AOI %s from %v to %v", area.AOIID, area.StartTime, area.EndTime)
	scenes, err := c.ScenesInventory(ctx, &area, *aoi)
	if err != nil {
		return nil, fmt.Errorf("DoScenesInventory.%w", err)
	}

	runtime.KeepAlive(aoi)

	return scenes, nil
}

// DoTilesInventory creates an inventory of all the tiles of the given scenes
func (c *Catalog) DoTilesInventory(ctx context.Context, area entities.AreaToIngest, scenes []*entities.Scene, rootLeaf []common.Tile) (int, error) {
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case entities.Sentinel1:
		// geos AOI
		aoi, err := geos.FromWKT(wkt.MustEncode(area.AOI.Geometry))
		if err != nil {
			return 0, fmt.Errorf("DoTilesInventory.FromWKT: %w", err)
		}

		log.Logger(ctx).Debug("Create burst inventory")
		scenes, burstsNb, err := c.BurstsInventory(ctx, *aoi, scenes)
		if err != nil {
			return 0, fmt.Errorf("DoTilesInventory.%w", err)
		}

		log.Logger(ctx).Debug("Append previous ingested scenes")
		ingestedScenes, err := c.IngestedScenesInventoryFromTile(ctx, rootLeaf)
		if err != nil {
			return 0, fmt.Errorf("DoTilesInventory.%w", err)
		}
		scenes = append(scenes, ingestedScenes...)

		log.Logger(ctx).Debug("Sort burst inventory")
		nTracks := c.BurstsSort(ctx, scenes)
		log.Logger(ctx).Sugar().Debugf("%d bursts found in %d tracks", burstsNb, nTracks)

		runtime.KeepAlive(aoi)

	case entities.Sentinel2:
		for _, scene := range scenes {
			scene.Tiles = append(scene.Tiles, &entities.Tile{
				TileLite: entities.TileLite{
					SourceID: scene.SourceID,
					SceneID:  scene.SourceID,
					Date:     scene.Data.Date,
				},
				GeometryWKT: scene.GeometryWKT,
			})
			scene.Data.TileMappings[scene.SourceID] = common.TileMapping{}
		}
	}

	// Set graphname & count tiles
	tilesNb := 0
	for _, scene := range scenes {
		for _, tile := range scene.Tiles {
			tile.Data.GraphName = area.TileGraphName
		}
		tilesNb += len(scene.Tiles)
	}

	return tilesNb, nil
}

// DeletePendingRecords deletes records of scenes that have not been successfully posted to the workflow server
func (c *Catalog) DeletePendingRecords(ctx context.Context, scenes []*entities.Scene, scenesID map[string]int) {
	// Delete records of scenes that have not been successfully posted to the workflow server
	var ids []string
	for _, s := range scenes {
		if _, ok := scenesID[s.SourceID]; !ok && s.Data.RecordID != "" && s.OwnRecord {
			ids = append(ids, s.Data.RecordID)
		}
	}
	if _, e := c.GeocubeClient.DeleteRecords(ids); e != nil {
		log.Logger(ctx).Sugar().Warnf("Catalog.IngestScenes : unable to delete unused records (%v): %v", ids, e)
	}
}
