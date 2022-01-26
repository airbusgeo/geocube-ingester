package workflow

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

func (wf *Workflow) CatalogHandler(r *mux.Router) {
	r.HandleFunc("/catalog/scenes", wf.CatalogScenesHandler).Methods("GET")
	r.HandleFunc("/catalog/tiles", wf.CatalogTilesHandler).Methods("GET")
	r.HandleFunc("/catalog/aoi", wf.CatalogPostAOIHandler).Methods("POST")
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

func (wf *Workflow) loadArea(w http.ResponseWriter, req *http.Request, validate bool) (catalog.AreaToIngest, error) {
	area := catalog.AreaToIngest{}
	areaJSON, err := readField(req, areaJSONField)
	if err != nil || len(areaJSON) == 0 {
		w.WriteHeader(400)
		if err == nil {
			err = fmt.Errorf("missing required field: '%s' (application/json)", areaJSONField)
		}
		fmt.Fprintf(w, "%v", err)
		return area, err
	}
	if err := json.Unmarshal(areaJSON, &area); err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "%v\nJSON:\n%s", err, areaJSON)
		return area, err
	}
	if validate {
		return area, wf.catalog.ValidateArea(&area)
	}
	return area, nil
}

func loadScenes(w http.ResponseWriter, req *http.Request, field string, ignore_empty bool) ([]*catalog.Scene, error) {
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
		return scenes, err
	}
	if err := json.Unmarshal(scenesJSON, &scenes); err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "%s %v", scenesJSON, err)
		return scenes, err
	}
	return scenes, nil
}

func (wf *Workflow) findScenes(ctx context.Context, w http.ResponseWriter, area catalog.AreaToIngest) ([]*catalog.Scene, error) {
	scenes, err := wf.catalog.DoScenesInventory(ctx, area)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return nil, err
	}
	return scenes, nil
}

func (wf *Workflow) findTiles(ctx context.Context, w http.ResponseWriter, area catalog.AreaToIngest, scenes []*catalog.Scene) error {
	var rootLeafTiles []common.Tile
	var err error
	switch catalog.GetConstellation(area.SceneType.Constellation) {
	case catalog.Sentinel1:
		rootLeafTiles, err = wf.RootTiles(ctx, area.AOIID)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintf(w, "%v", err)
			return err
		}
		leafTiles, err := wf.LeafTiles(ctx, area.AOIID)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintf(w, "%v", err)
			return err
		}
		rootLeafTiles = append(rootLeafTiles, leafTiles...)
	}

	if _, err = wf.catalog.DoTilesInventory(ctx, area, scenes, rootLeafTiles); err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return err
	}
	return nil
}

func (wf *Workflow) postScenes(ctx context.Context, area catalog.AreaToIngest, scenes []*catalog.Scene) (map[string]int, error) {
	ids := map[string]int{}

	// Prepare scenes
	scenesToIngest, err := wf.catalog.ScenesToIngest(ctx, area, scenes)
	if err != nil {
		return ids, err
	}

	// First, create AOI
	if err := wf.CreateAOI(ctx, area.AOIID); err != nil && !errors.As(err, &db.ErrAlreadyExists{}) {
		return ids, err
	}

	// Then, create scenes
	for _, scene := range scenesToIngest {
		nid, err := wf.IngestScene(ctx, area.AOIID, scene)
		if err != nil {
			if !errors.As(err, &db.ErrAlreadyExists{}) {
				return ids, err
			}
		} else {
			ids[scene.SourceID] = nid
		}
	}
	return ids, nil
}

// CatalogScenesHandler lists scenes for a given AOI, satellites and interval of time and returns a json
func (wf *Workflow) CatalogScenesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	area, err := wf.loadArea(w, req, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogScenesHandler: %v", err)
		return
	}

	scenes, err := wf.findScenes(ctx, w, area)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogScenesHandler.%v", err)
		return
	}

	json.NewEncoder(w).Encode(catalog.Scenes(scenes))
}

// CatalogTilesHandler lists tiles for a given list of scenes (previous call of CatalogScenesHandler)
func (wf *Workflow) CatalogTilesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	area, err := wf.loadArea(w, req, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogTilesHandler: %v", err)
		return
	}

	scenes, err := loadScenes(w, req, scenesJSONField, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogTilesHandler: %v", err)
		return
	}

	if err = wf.findTiles(ctx, w, area, scenes); err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogTilesHandler: %v", err)
		return
	}

	json.NewEncoder(w).Encode(catalog.Scenes(scenes))
}

// CatalogPostAOIHandler posts a request to ingest scenes
func (wf *Workflow) CatalogPostAOIHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	area, err := wf.loadArea(w, req, true)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogPostAOIHandler: %v", err)
		return
	}

	var scenes []*catalog.Scene
	// Try to load tiles
	if scenes, err = loadScenes(w, req, tilesJSONField, true); err != nil {
		// Or try to load scenes
		if scenes, err = loadScenes(w, req, scenesJSONField, true); err != nil {
			// Or find scenes
			if scenes, err = wf.findScenes(ctx, w, area); err != nil {
				log.Logger(ctx).Sugar().Warnf("wf.CatalogPostAOIHandler.%v", err)
				return
			}
		}
		// Then find tiles
		if err = wf.findTiles(ctx, w, area, scenes); err != nil {
			log.Logger(ctx).Sugar().Warnf("wf.CatalogPostAOIHandler: %v", err)
			return
		}
	}

	// Post scenes
	ids := map[string]int{}
	defer func() {
		wf.catalog.DeletePendingRecords(ctx, scenes, ids)
	}()

	if ids, err = wf.postScenes(ctx, area, scenes); err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.CatalogPostAOIHandler.%v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}

	w.WriteHeader(200)
	tiles := 0
	for _, scene := range scenes {
		if _, ok := ids[scene.SourceID]; ok {
			tiles += len(scene.Tiles)
		}
	}

	fmt.Fprintf(w, "Ingestion of %d scenes and %d tiles in progress\n", len(ids), tiles)
}
