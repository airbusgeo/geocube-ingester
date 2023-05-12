package catalog

import (
	"context"
	"fmt"
	"regexp"
	"runtime"

	geocube "github.com/airbusgeo/geocube-client-go/client"
	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

// Catalog is the main class of this package
type Catalog struct {
	GeocubeClient                  *geocube.Client
	Workflow                       WorkflowManager
	ScihubUser                     string
	ScihubPword                    string
	OneAtlasCatalogUser            string
	OneAtlasApikey                 string
	OneAtlasCatalogEndpoint        string
	OneAtlasOrderEndpoint          string
	OneAtlasAuthenticationEndpoint string
	GCSAnnotationsBucket           string
	WorkingDir                     string
}

func (c *Catalog) ValidateArea(ctx context.Context, area *entities.AreaToIngest) error {
	// Check AOI ID
	matched, err := regexp.MatchString("^[a-zA-Z0-9-:_]+([a-zA-Z0-9-:_]+)*$", area.AOIID)
	if err != nil {
		return fmt.Errorf("validateArea.AOI: %w", err)
	}
	if !matched {
		return fmt.Errorf("validateArea: wrong format for AOIID (must be chars, numbers and -:_ found '%s')", area.AOIID)
	}

	// Check constellation
	if entities.GetConstellation(area.SceneType.Constellation) == common.Unknown {
		return fmt.Errorf("validateArea: unrecognized constellation: %s", area.SceneType.Constellation)
	}

	if c.GeocubeClient == nil {
		return fmt.Errorf("validateArea: no connection to the geocube")
	}

	// Check that instances exist
	for k, layer := range area.Layers {
		if layer.InstanceID != "" {
			if _, err := c.GeocubeClient.GetVariableFromInstanceID(ctx, layer.InstanceID); err != nil {
				return fmt.Errorf("validateArea: %w", err)
			}
		} else {
			v, err := c.GeocubeClient.GetVariableFromName(ctx, layer.Variable)
			if err != nil {
				return fmt.Errorf("validateArea: %w", err)
			}
			vi := v.Instance(layer.Instance)
			if vi == nil {
				return fmt.Errorf("validateArea: unknown instance %s for variable %s", layer.Instance, layer.Variable)
			}
			layer.InstanceID = vi.InstanceID
			area.Layers[k] = layer
		}
	}
	return nil
}

// DoScenesInventory lists scenes for a given AOI, satellites and interval of time
func (c *Catalog) DoScenesInventory(ctx context.Context, area entities.AreaToIngest) (entities.Scenes, error) {
	// geos AOI
	aoi, err := geos.FromWKT(wkt.MustEncode(area.AOI))
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("DoScenesInventory.FromWKT: %w", err)
	}

	// Search scenes covering this area
	log.Logger(ctx).Sugar().Debugf("Search scenes for AOI %s from %v to %v", area.AOIID, area.StartTime, area.EndTime)
	scenes, err := c.ScenesInventory(ctx, &area, *aoi)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("DoScenesInventory.%w", err)
	}

	runtime.KeepAlive(aoi)

	return scenes, nil
}

// DoTilesInventory creates an inventory of all the tiles of the given scenes
func (c *Catalog) DoTilesInventory(ctx context.Context, area entities.AreaToIngest, scenes entities.Scenes, rootLeaf []common.Tile) (int, error) {
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		if area.SceneGraphName == common.GraphCopyProductToStorage {
			// No bursts inventory if we copy the product to the storage
			for _, scene := range scenes.Scenes {
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
		} else {
			// burst inventory intersecting AOI
			aoi, err := geos.FromWKT(wkt.MustEncode(area.AOI))
			if err != nil {
				return 0, fmt.Errorf("DoTilesInventory.FromWKT: %w", err)
			}

			log.Logger(ctx).Debug("Create burst inventory")
			scenes, burstsNb, err := c.BurstsInventory(ctx, area, *aoi, scenes.Scenes)
			if err != nil {
				return 0, fmt.Errorf("DoTilesInventory.%w", err)
			}

			log.Logger(ctx).Debug("Append previous ingested scenes")
			ingestedScenes, err := c.IngestedScenesInventoryFromTiles(ctx, rootLeaf)
			if err != nil {
				return 0, fmt.Errorf("DoTilesInventory.%w", err)
			}
			scenes = append(scenes, ingestedScenes...)

			log.Logger(ctx).Debug("Sort burst inventory")
			nTrackSwaths := c.BurstsSort(ctx, scenes)
			log.Logger(ctx).Sugar().Debugf("%d bursts found in %d tracks and swaths", burstsNb, nTrackSwaths)

			runtime.KeepAlive(aoi)
		}

	case common.Sentinel2, common.SPOT, common.PHR:
		for _, scene := range scenes.Scenes {
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
	for _, scene := range scenes.Scenes {
		for _, tile := range scene.Tiles {
			tile.Data.GraphName = area.TileGraphName
		}
		tilesNb += len(scene.Tiles)
	}

	return tilesNb, nil
}

// DeletePendingRecords deletes records of scenes that have not been successfully posted to the workflow server
func (c *Catalog) DeletePendingRecords(ctx context.Context, scenes entities.Scenes, scenesID map[string]int) {
	if c.GeocubeClient == nil {
		return
	}

	// Delete records of scenes that have not been successfully posted to the workflow server
	var ids []string
	for _, s := range scenes.Scenes {
		if _, ok := scenesID[s.SourceID]; !ok && s.Data.RecordID != "" && s.OwnRecord {
			ids = append(ids, s.Data.RecordID)
		}
	}
	if _, e := c.GeocubeClient.DeleteRecords(ctx, ids); e != nil {
		log.Logger(ctx).Sugar().Warnf("Catalog.IngestScenes : unable to delete unused records (%v): %v", ids, e)
	}
}

// IngestAreaResult is the output of the catalog
type IngestAreaResult struct {
	ScenesID map[string]int `json:"scenes_id"`
	TilesNb  int            `json:"tiles_nb"`
}

func (c *Catalog) IngestArea(ctx context.Context, area entities.AreaToIngest, scenes, scenesWithTiles entities.Scenes, outputDir string) (IngestAreaResult, error) {
	var (
		err            error
		scenesToIngest []common.SceneToIngest
		result         IngestAreaResult
	)

	if err := c.ValidateArea(ctx, &area); err != nil {
		return result, fmt.Errorf("IngestArea.%w", err)
	}

	// Scene inventory
	if scenes.Scenes == nil && scenesWithTiles.Scenes == nil {
		if scenes, err = c.DoScenesInventory(ctx, area); err != nil {
			return result, fmt.Errorf("ingestArea.%w", err)
		}
		service.ToJSON(struct{ Scenes entities.Scenes }{Scenes: scenes}, outputDir, "scenesInventory.json")
	}

	// Tile inventory
	if scenesWithTiles.Scenes == nil {
		result.TilesNb, err = c.FindTiles(ctx, area, scenes)
		if err != nil {
			return result, fmt.Errorf("ingestArea.%w", err)
		}
		service.ToJSON(struct{ Scenes entities.Scenes }{Scenes: scenes}, outputDir, "tilesInventory.json")
	} else {
		scenes = scenesWithTiles
	}

	defer func() {
		c.DeletePendingRecords(ctx, scenes, result.ScenesID)
	}()

	// Create scenes to ingest
	log.Logger(ctx).Debug("Create scenes to ingest")
	scenesToIngest, err = c.ScenesToIngest(ctx, area, scenes)
	if err != nil {
		return result, fmt.Errorf("ingestArea.%w", err)
	}
	service.ToJSON(struct{ Scenes []common.SceneToIngest }{Scenes: scenesToIngest}, outputDir, "scenesToIngest.json")

	// Post scenes
	log.Logger(ctx).Debug("Post scenes to ingest")
	if result.ScenesID, err = c.PostScenes(ctx, area, scenesToIngest); err != nil {
		return result, fmt.Errorf("ingestArea.%w", err)
	}
	log.Logger(ctx).Debug("Done !")

	result.TilesNb = 0
	for _, scene := range scenes.Scenes {
		if _, ok := result.ScenesID[scene.SourceID]; ok {
			result.TilesNb += len(scene.Tiles)
		}
	}

	return result, err
}
