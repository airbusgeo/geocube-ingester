package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	catalog "github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	db "github.com/airbusgeo/geocube-ingester/interface/database"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/gorilla/mux"
)

const areaJSONField = "area"
const scenesJSONField = "scenes"
const tilesJSONField = "tiles"

func (c *Catalog) AddHandler(r *mux.Router) {
	r.HandleFunc("/catalog/scenes", c.ScenesHandler).Methods("GET")
	r.HandleFunc("/catalog/tiles", c.TilesHandler).Methods("GET")
	r.HandleFunc("/catalog/scenes", c.ScenesHandler).Methods("POST")
	r.HandleFunc("/catalog/tiles", c.TilesHandler).Methods("POST")
	r.HandleFunc("/catalog/aoi", c.PostAOIHandler).Methods("POST")
}

func readField(req *http.Request, field string) ([]byte, error) {
	if req.FormValue(field) != "" {
		return []byte(req.FormValue(field)), nil
	}
	file, _, err := req.FormFile(field)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf bytes.Buffer
	io.Copy(&buf, file)
	return buf.Bytes(), nil
}

func (c *Catalog) loadArea(req *http.Request) (catalog.AreaToIngest, error) {
	area := catalog.AreaToIngest{}
	areaJSON, err := readField(req, areaJSONField)
	if err != nil {
		return area, err
	}
	if len(areaJSON) == 0 {
		return area, fmt.Errorf("loadArea: missing required field: '%s' (application/json)", areaJSONField)
	}
	if err := json.Unmarshal(areaJSON, &area); err != nil {
		return area, fmt.Errorf("loadArea: %w\nJSON:\n%s", err, areaJSON)
	}
	return area, nil
}

func loadScenes(w http.ResponseWriter, req *http.Request, field string, ignore_empty bool) (catalog.Scenes, error) {
	scenes := catalog.Scenes{}

	scenesJSON, err := readField(req, field)
	if err != nil || len(scenesJSON) == 0 {
		if !ignore_empty {
			w.WriteHeader(400)
			if err != nil {
				err = fmt.Errorf("missing required field: '%s' (application/json)", field)
			}
			fmt.Fprintf(w, "%v", err)
		}
		return catalog.Scenes{}, err
	}
	if err := json.Unmarshal(scenesJSON, &scenes); err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "%s %v", scenesJSON, err)
		return catalog.Scenes{}, err
	}
	return scenes, nil
}

func (c *Catalog) FindTiles(ctx context.Context, area catalog.AreaToIngest, scenes catalog.Scenes) (int, error) {
	var rootLeafTiles []common.Tile
	var err error
	switch catalog.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		if c.Workflow == nil {
			return 0, fmt.Errorf("FindTiles: WorkflowServer is not defined")
		}
		if rootLeafTiles, err = c.Workflow.RootTiles(ctx, area.AOIID); err != nil {
			return 0, err
		}
		leafTiles, err := c.Workflow.LeafTiles(ctx, area.AOIID)
		if err != nil {
			return 0, err
		}
		rootLeafTiles = append(rootLeafTiles, leafTiles...)
	}

	tileNb, err := c.DoTilesInventory(ctx, area, scenes, rootLeafTiles)
	if err != nil {
		return 0, err
	}
	return tileNb, nil
}

// PostScenes sends the scenes to the workflow server
// Returns the id of the posted scenes (even if PostScenesToIngest returns an error)
func (c Catalog) PostScenes(ctx context.Context, area catalog.AreaToIngest, scenesToIngest []common.SceneToIngest) (map[string]int, error) {
	if c.Workflow == nil {
		return nil, fmt.Errorf("postScenes: WorkflowServer is not defined")
	}

	ids := map[string]int{}

	// First, create AOI
	if err := c.Workflow.CreateAOI(ctx, area.AOIID); err != nil && !errors.As(err, &db.ErrAlreadyExists{}) {
		return ids, fmt.Errorf("postScenes.%w", err)
	}

	// Then, create scenes
	for _, scene := range scenesToIngest {
		nid, err := c.Workflow.IngestScene(ctx, area.AOIID, scene)
		if err != nil {
			if errors.As(err, &db.ErrAlreadyExists{}) {
				continue
			}
			return ids, fmt.Errorf("postScenes.%w", err)
		}
		ids[scene.SourceID] = nid
	}
	return ids, nil
}

// ScenesHandler lists scenes for a given AOI, satellites and interval of time and returns a json
func (c Catalog) ScenesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	area, err := c.loadArea(req)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "%v", err)
		return
	}

	scenes, err := c.DoScenesInventory(ctx, area)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ScenesHandler.%v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}

	if err := json.NewEncoder(w).Encode(catalog.Scenes(scenes)); err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ScenesHandler.%v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
	}
}

// TilesHandler lists tiles for a given list of scenes (previous call of ScenesHandler)
func (c Catalog) TilesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	area, err := c.loadArea(req)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "%v", err)
		return
	}

	scenes, err := loadScenes(w, req, scenesJSONField, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.TilesHandler: %v", err)
		return
	}

	if _, err = c.FindTiles(ctx, area, scenes); err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.TilesHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}

	if err := json.NewEncoder(w).Encode(catalog.Scenes(scenes)); err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ScenesHandler.%v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
	}
}

// PostAOIHandler posts a request to ingest scenes
func (c Catalog) PostAOIHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	area, err := c.loadArea(req)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "%v", err)
		return
	}

	var scenes, tiles catalog.Scenes
	// Try to load tiles
	if tiles, err = loadScenes(w, req, tilesJSONField, true); err != nil {
		// Or try to load scenes
		scenes, _ = loadScenes(w, req, scenesJSONField, true)
	}

	result, err := c.IngestArea(ctx, area, scenes, tiles, "")
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.PostAOIHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}

	w.WriteHeader(200)
	fmt.Fprintf(w, "Ingestion of %d scenes and %d tiles in progress\n", len(result.ScenesID), result.TilesNb)
}
