package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/interface/catalog/copernicus"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/creodias"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/oneatlas"

	geocube "github.com/airbusgeo/geocube-client-go/client"
	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/catalog"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/geometry"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
	"google.golang.org/grpc/codes"
)

// ScenesInventory makes an inventory of all the scenes covering the area between startDate and endDate
// The scenes are retrieved from different providers
func (c *Catalog) ScenesInventory(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	// Search
	constellation := common.GetConstellationFromString(area.SceneType.Constellation)
	var sceneProviders []catalog.ScenesProvider
	if c.CreodiasCatalog && constellation == common.Sentinel2 {
		sceneProviders = append(sceneProviders, &creodias.Provider{}) // Prefered provider for Sentinel2
	}
	if c.CopernicusCatalog {
		sceneProviders = append(sceneProviders, &copernicus.Provider{})
	}
	if c.CreodiasCatalog {
		sceneProviders = append(sceneProviders, &creodias.Provider{})
	}
	if c.OneAtlasCatalogUser != "" {
		oneAtlasProvider, oneAtlasProviderCncl := oneatlas.NewOneAtlasProvider(ctx,
			c.OneAtlasCatalogUser,
			c.OneAtlasApikey,
			c.OneAtlasCatalogEndpoint,
			c.OneAtlasOrderEndpoint,
			c.OneAtlasAuthenticationEndpoint)
		if oneAtlasProvider.Supports(constellation) {
			sceneProviders = append(sceneProviders, oneAtlasProvider)
			defer oneAtlasProviderCncl()
		} else {
			oneAtlasProviderCncl()
		}
	}
	if len(sceneProviders) == 0 {
		return entities.Scenes{}, fmt.Errorf("no catalog is configured for '%s'", area.SceneType.Constellation)
	}

	var err, e error
	var scenes entities.Scenes
	for _, sceneProvider := range sceneProviders {
		if !sceneProvider.Supports(constellation) {
			continue
		}
		scenes, e = sceneProvider.SearchScenes(ctx, area, aoi)
		if err = service.MergeErrors(false, err, e); err == nil {
			break
		}
	}
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("ScenesInventory.%w", err)
	}

	// Define common attributes:
	for _, scene := range scenes.Scenes {
		scene.AOI = area.AOIID
		scene.Data.GraphName = area.SceneGraphName
		scene.Data.GraphConfig = area.GraphConfig
		scene.Data.IsRetriable = area.IsRetriable
		scene.Data.StorageURI = area.StorageURI

		// Copy area tags
		for k, v := range area.RecordTags {
			if _, ok := scene.Tags[k]; !ok {
				scene.Tags[k] = v
			}
		}
	}

	if len(area.AnnotationsURLs) == 1 {
		for i, s := range scenes.Scenes {
			if scenes.Scenes[i].Data.Metadata == nil {
				scenes.Scenes[i].Data.Metadata = map[string]interface{}{}
			}
			if dl, ok := scenes.Scenes[i].Data.Metadata[common.DownloadLinkMetadata].(string); !ok || dl == "" {
				if info, err := common.Info(s.SourceID); err == nil {
					scenes.Scenes[i].Data.Metadata[common.DownloadLinkMetadata] = common.FormatBrackets(area.AnnotationsURLs[0], info, map[string]string{"AREA": area.AOIID})
				}
			}
		}
	}

	// Refine inventory
	scenesCount := len(scenes.Scenes)
	scenes.Scenes, err = refineInventory(area, scenes.Scenes, aoi)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("ScenesInventory.%w", err)
	}

	if scenes.Properties == nil {
		scenes.Properties = map[string]string{}
	}
	if area.Limit > 0 && scenesCount == area.Limit {
		scenes.Properties["next"] = "true"
	} else {
		scenes.Properties["next"] = "false"
	}

	log.Logger(ctx).Sugar().Debugf("%d scenes found (%d filtered out)", len(scenes.Scenes), scenesCount-len(scenes.Scenes))

	return scenes, nil
}

// IngestedScenesInventoryFromTiles retrieves the ingested scenes from a list of tiles
func (c *Catalog) IngestedScenesInventoryFromTiles(ctx context.Context, tiles []common.Tile) ([]*entities.Scene, error) {
	scenesID := map[string]*entities.Scene{}
	var scenes []*entities.Scene
	for _, tile := range tiles {
		scene, ok := scenesID[tile.Scene.SourceID]
		if !ok {
			scene = &entities.Scene{Scene: common.Scene{SourceID: tile.Scene.SourceID}, Ingested: true}
			scenesID[tile.Scene.SourceID] = scene
			scenes = append(scenes, scene)
		}
		tile := &entities.Tile{
			TileLite: entities.TileLite{
				SourceID: tile.SourceID,
				SceneID:  tile.Scene.SourceID,
				Date:     tile.Scene.Data.Date,
			},
			Ingested: true,
			Data:     tile.Data,
		}
		if common.GetConstellationFromString(scene.SourceID) == common.Sentinel1 {
			tile.AnxTime, _ = strconv.Atoi(strings.Split(tile.SourceID, "_")[2])
		}
		scene.Tiles = append(scene.Tiles, tile)
	}

	return scenes, nil
}

// ScenesToIngest creates the payload for each scene
func (c *Catalog) ScenesToIngest(ctx context.Context, area entities.AreaToIngest, scenes entities.Scenes) ([]common.SceneToIngest, error) {
	var scenesToIngest []common.SceneToIngest

	if len(scenes.Scenes) == 0 {
		return scenesToIngest, nil
	}

	if c.GeocubeClient == nil {
		return nil, fmt.Errorf("scenesToIngest: no connection to the geocube")
	}

	if err := c.ValidateArea(ctx, &area); err != nil {
		return nil, fmt.Errorf("scenesToIngest.%w", err)
	}
	instances := area.InstancesID()

	// Get Union of scenes AOIs
	var wkts []string
	for _, scene := range scenes.Scenes {
		wkts = append(wkts, scene.GeometryWKT)
	}
	wktAoi, err := geometry.WKTUnion(wkts, geometry.TOLERANCE_GEOG)
	if err != nil {
		return nil, fmt.Errorf("scenesToIngest.%w", err)
	}
	geocubeAOI, err := wktToGeocubeAOI(wktAoi)
	if err != nil {
		return nil, fmt.Errorf("scenesToIngest.%w", err)
	}

	recordsList, err := c.GeocubeClient.ListRecords(ctx, "", area.RecordTags, geocubeAOI,
		slices.MinFunc(scenes.Scenes, func(s1, s2 *entities.Scene) int { return s1.Data.Date.Compare(s2.Data.Date) }).Data.Date,
		slices.MaxFunc(scenes.Scenes, func(s1, s2 *entities.Scene) int { return s1.Data.Date.Compare(s2.Data.Date) }).Data.Date,
		0, 0, false)
	if err != nil {
		return nil, fmt.Errorf("scenesToIngest.ListRecords: %w", err)
	}
	records := map[string][]*geocube.Record{}
	for _, r := range recordsList {
		delete(r.Tags, common.TagProcessingDate)
		records[r.Name] = append(records[r.Name], r)
	}

	scenesToCreateRecord := map[int]*entities.Scene{} // Store the scenes that need to create their records

	for _, scene := range scenes.Scenes {
		if len(scene.Tiles) == 0 || scene.Ingested {
			continue
		}
		// Check that the records of the area are the same as the one of the scene
		for k, v := range area.RecordTags {
			if vs, ok := scene.Tags[k]; !ok || v != vs {
				return nil, fmt.Errorf("scenesToIngest.CheckRecordTags: area and scene '%s' have different value for tag '%s': '%s' != '%s'", scene.SourceID, k, v, vs)
			}
		}

		// Create sceneToIngest
		sceneToIngest := common.SceneToIngest{
			Scene:      scene.Scene,
			Tiles:      map[string]common.TileToIngest{},
			RetryCount: area.RetryCount,
		}
		prevScenes := service.StringSet{}
		refScenes := service.StringSet{}
		for _, tile := range scene.Tiles {
			t := common.TileToIngest{
				Data: tile.Data,
			}
			if tile.Previous != nil {
				t.PreviousTileID = tile.Previous.SourceID
				t.PreviousSceneID = tile.Previous.SceneID
				prevScenes.Push(tile.Previous.SceneID)
			}
			if tile.Reference != nil {
				t.ReferenceTileID = tile.Reference.SourceID
				t.ReferenceSceneID = tile.Reference.SceneID
				refScenes.Push(tile.Reference.SceneID)
			}
			sceneToIngest.Tiles[tile.SourceID] = t
		}

		// Add prev&ref scenes to tags
		refScenesb, err := json.Marshal(refScenes.Slice())
		if err != nil {
			return nil, fmt.Errorf("scenesToIngest.Marshal: %w", err)
		}
		scene.Tags[common.TagRefScenes] = string(refScenesb)
		prevScenesb, err := json.Marshal(prevScenes.Slice())
		if err != nil {
			return nil, fmt.Errorf("scenesToIngest.Marshal: %w", err)
		}
		scene.Tags[common.TagPrevScenes] = string(prevScenesb)

		// Find if the record already exists
		if recordsList, ok := records[scene.SourceID]; ok {
			for _, r := range recordsList {
				if r.Time == scene.Data.Date && reflect.DeepEqual(r.Tags, scene.Tags) {
					scene.Data.RecordID = r.ID
					break
				}
			}
		}
		if scene.Data.RecordID == "" {
			scenesToCreateRecord[len(scenesToIngest)] = scene // Will be created later
			scene.OwnRecord = true
		} else {
			sceneToIngest.Scene.Data.RecordID = scene.Data.RecordID
		}
		sceneToIngest.Scene.Data.InstancesID = instances
		scenesToIngest = append(scenesToIngest, sceneToIngest)
	}

	// Create all the records at once
	if err := c.createRecords(ctx, scenesToCreateRecord); err != nil {
		return nil, fmt.Errorf("sceneToIngest.%w", err)
	}
	for i, scene := range scenesToCreateRecord {
		scenesToIngest[i].Scene.Data.RecordID = scene.Data.RecordID
	}

	// Sort by dates
	sort.Slice(scenesToIngest, func(i, j int) bool { return scenesToIngest[i].Data.Date.Before(scenesToIngest[j].Data.Date) })

	return scenesToIngest, nil
}

func refineInventory(area *entities.AreaToIngest, scenes []*entities.Scene, aoi geos.Geometry) ([]*entities.Scene, error) {
	var err error
	scenes = removeDoubleEntries(scenes)
	if scenes, err = removeOutsideAOI(scenes, aoi); err != nil {
		return nil, fmt.Errorf("refineInventory.%w", err)
	}
	if common.GetConstellationFromString(area.SceneType.Constellation) == common.Sentinel1 {
		if scenes, err = handleEquatorCrossing(scenes); err != nil {
			return nil, fmt.Errorf("refineInventory.%w", err)
		}
		if err = handleNonContinuousSwath(scenes); err != nil {
			return nil, fmt.Errorf("refineInventory.%w", err)
		}
	}
	return scenes, nil
}

// removeDoubleEntries removes acquisitions that appear twice in the inventory
// The last 4 digits of the re-processed scene identifier then change. When searching for data, both scenes will be found, even though they are the same.
// This routine checks of such appearance and selects the latest product.
// Credit: OpenSarToolkit
func removeDoubleEntries(scenes []*entities.Scene) []*entities.Scene {
	identifiers := map[string]int{}

	j := 0
	for _, scene := range scenes {
		if k, ok := identifiers[scene.ProductName]; !ok {
			scenes[j] = scene
			identifiers[scene.ProductName] = j
			j++
		} else if scenes[k].Tags[common.TagIngestionDate] < scene.Tags[common.TagIngestionDate] {
			scenes[k] = scene
		}
	}

	return scenes[0:j]
}

// removeOutsideAOI removes scenes that are located outside the AOI
// The search routine works over a simplified representation of the AOI.
// This may then include acquisitions that do not overlap with the AOI.
// In this step we sort out the scenes that are completely outside the actual AOI.
// Credit: OpenSarToolkit
func removeOutsideAOI(scenes []*entities.Scene, aoi geos.Geometry) ([]*entities.Scene, error) {
	// Prepare geometry for intersection
	paoi := aoi.Prepare()

	j := 0
	for i, scene := range scenes {
		aoiScene, err := geos.FromWKT(scene.GeometryWKT)
		if err != nil {
			return nil, fmt.Errorf("removeOutsideAOI.FromWKT: %w", err)
		}
		intersect, err := paoi.Intersects(aoiScene)
		if err != nil {
			return nil, fmt.Errorf("removeOutsideAOI.Intersects: %w", err)
		}
		if intersect {
			scenes[j] = scenes[i]
			j++
		}
	}
	runtime.KeepAlive(aoi)

	return scenes[0:j], nil
}

// handleEquatorCrossing adjusts track number when crossing the equator
// For ascending tracks crossing the equator, the relative orbit will increase by 1.
// This routine checks for the appearance of such kind and unifies the relativeorbitnumbers
// so that the inventory is compliant with the subsequent batch processing routines
// Credit: OpenSarToolkit
func handleEquatorCrossing(scenes []*entities.Scene) ([]*entities.Scene, error) {
	// Get the relativeorbitnumbers that change with equator crossing
	relativeOrbits := service.StringSet{}
	for _, scene := range scenes {
		if lastrelative, ok := scene.Tags[common.TagLastRelativeOrbit]; ok && !relativeOrbits.Exists(scene.Tags[common.TagRelativeOrbit]) && scene.Tags[common.TagRelativeOrbit] != lastrelative {
			relativeOrbits.Push(scene.Tags[common.TagRelativeOrbit])
		}
	}

	if len(relativeOrbits) != 0 {
		return nil, fmt.Errorf("found %d equator crossing orbits... This has never been tested", len(relativeOrbits))
	}
	return scenes, nil
}

// handleNonContinuousSwath removes incomplete tracks with respect to the AOI
// In some cases the AOI is covered by 2 different parts of the same track. We assumes that acquisitions with the same "relative orbit" (i.e. track)
// should be merged. However, SNAP will fail when slices of acquisitions are missing in between. Therefore this routine renames the tracks into
// XXX_1, XXX_2, XXX_n, dependent on the number of segments.
// Credit: OpenSarToolkit
func handleNonContinuousSwath(scenes []*entities.Scene) error {
	scenesPerTrackPerDate := map[string]map[time.Time][]*entities.Scene{}
	for _, scene := range scenes {
		m := scenesPerTrackPerDate[scene.Tags[common.TagLastRelativeOrbit]]
		if m == nil {
			m = map[time.Time][]*entities.Scene{}
		}
		m[scene.Data.Date] = append(m[scene.Data.Date], scene)

		scenesPerTrackPerDate[scene.Tags[common.TagLastRelativeOrbit]] = m
	}

	for _, mscenes := range scenesPerTrackPerDate {
		for _, lscenes := range mscenes {
			if len(lscenes) > 1 {
				minSlice, err := strconv.Atoi(lscenes[0].Tags[common.TagSliceNumber])
				if err != nil {
					return fmt.Errorf("handleNonContinuousSwath: %w", err)
				}
				slices := map[int]struct{}{minSlice: {}}
				maxSlice := minSlice
				for i := 1; i < len(lscenes); i++ {
					slice, err := strconv.Atoi(lscenes[i].Tags[common.TagSliceNumber])
					if err != nil {
						return fmt.Errorf("handleNonContinuousSwath: %w", err)
					}
					if slice > maxSlice {
						maxSlice = slice
					}
					if slice < minSlice {
						minSlice = slice
					}
					slices[slice] = struct{}{}
				}

				if len(slices) > maxSlice-minSlice+1 {
					return fmt.Errorf("nonContinuousSwath is not handle right now")
				}
			}
		}
	}
	return nil
}

// createRecords for the scenes
func (c *Catalog) createRecords(ctx context.Context, scenes map[int]*entities.Scene) error {
	if c.GeocubeClient == nil {
		return fmt.Errorf("createRecords: no connection to the geocube")
	}
	names := make([]string, len(scenes))
	aois := make([]string, len(scenes))
	tags := make([]map[string]string, len(scenes))
	dates := make([]time.Time, len(scenes))
	ind := make([]int, len(scenes))
	i := 0
	for j, scene := range scenes {
		ind[i] = j
		names[i] = scene.SourceID
		tags[i] = scene.Tags
		dates[i] = scene.Data.Date
		// CreateAOI
		{
			aoi, err := wktToGeocubeAOI(scene.GeometryWKT)
			if err != nil {
				return fmt.Errorf("CreateRecords.%w", err)
			}
			if aois[i], err = c.GeocubeClient.CreateAOI(ctx, aoi); err != nil && geocube.Code(err) != codes.AlreadyExists {
				return fmt.Errorf("CreateRecords.%w", err)
			}
		}
		i++
	}

	// CreateRecords
	recordsId, err := c.GeocubeClient.CreateRecords(ctx, names, aois, dates, tags)
	if err != nil {
		return fmt.Errorf("CreateRecords.%w", err)
	}
	for i, r := range recordsId {
		scenes[ind[i]].Data.RecordID = r
	}
	return nil
}

func wktToGeocubeAOI(wktAOI string) (geocube.AOI, error) {
	geo, err := wkt.DecodeString(wktAOI)
	if err != nil {
		return nil, fmt.Errorf("wktToGeocubeAOI: %w", err)
	}
	var mp [][][][2]float64
	switch g := geo.(type) {
	case geom.Polygoner:
		mp = [][][][2]float64{g.LinearRings()}
	case geom.MultiPolygoner:
		mp = g.Polygons()
	default:
		return geocube.AOI{}, fmt.Errorf("unsupported geometry: %v", g)
	}

	for i, polygon := range mp {
		for j, linearring := range polygon {
			if linearring[0][0] != linearring[len(linearring)-1][0] || linearring[0][1] != linearring[len(linearring)-1][1] {
				mp[i][j] = append(mp[i][j], mp[i][j][0])
			}
		}
	}
	return geocube.AOIFromMultiPolygonArray(mp), nil
}
