package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	db "github.com/airbusgeo/geocube-ingester/interface/database"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

func (wf *Workflow) NewRouter() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/", wf.GetStatus).Methods("GET")
	r.HandleFunc("/scene/{scene}", wf.GetSceneHandler).Methods("GET")
	r.HandleFunc("/scene/{scene}/tiles", wf.ListSceneTilesHandler).Methods("GET")
	r.HandleFunc("/scene/{scene}/retry", wf.RetrySceneHandler).Methods("PUT")
	r.HandleFunc("/scene/{scene}/fail", wf.FailSceneHandler).Methods("PUT")
	r.HandleFunc("/scene/{scene}/force/{status}", wf.ForceSceneStatusHandler).Methods("PUT")
	r.HandleFunc("/tile/{tile}", wf.GetTileHandler).Methods("GET")
	r.HandleFunc("/tile/{tile}/retry", wf.RetryTileHandler).Methods("PUT")
	r.HandleFunc("/tile/{tile}/fail", wf.FailTileHandler).Methods("PUT")
	r.HandleFunc("/tile/{tile}/force/{status}", wf.ForceTileStatusHandler).Methods("PUT")
	r.HandleFunc("/aoi/", wf.ListAOIsHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}", wf.GetAOIStatusHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}", wf.CreateAOIHandler).Methods("POST")
	r.HandleFunc("/aoi/{aoi}", wf.DeleteAOIHandler).Methods("DELETE")
	r.HandleFunc("/aoi/{aoi}/dot", wf.PrintDotHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}/scene", wf.CreateSceneHandler).Methods("POST")
	r.HandleFunc("/aoi/{aoi}/scenes", wf.ListScenesHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}/scenes/{status}", wf.ListScenesHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}/tiles/{status}", wf.ListAOITilesHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}/roottiles", wf.ListRootTilesHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}/leaftiles", wf.ListLeafTilesHandler).Methods("GET")
	r.HandleFunc("/aoi/{aoi}/retry", wf.RetryAOIHandler).Methods("PUT")
	r.HandleFunc("/aoi/{aoi}/retry/{force}", wf.RetryAOIHandler).Methods("PUT")
	return r
}

func (wf *Workflow) ResultHandler(ctx context.Context, result common.Result) error {
	var err error
	switch result.Type {
	case common.ResultTypeTile:
		_, err = wf.UpdateTileStatus(ctx, result.ID, result.Status, &result.Message, false)
	case common.ResultTypeScene:
		_, err = wf.UpdateSceneStatus(ctx, result.ID, result.Status, &result.Message, false)
	default:
		panic(result.Type)
	}
	return err
}

func ifElse(cond bool, valtrue, valfalse int) int {
	if cond {
		return valtrue
	}
	return valfalse
}

// GetStatus returns ok
func (wf *Workflow) GetStatus(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("ingester ok"))
}

// GetSceneHandler retrieves a scene
func (wf *Workflow) GetSceneHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	scstr := mux.Vars(req)["scene"]
	scene, err := strconv.Atoi(scstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	im, err := wf.Scene(ctx, scene, nil)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.scene: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(im)

}

// ListSceneTilesHandler lists the tiles of the scene
func (wf *Workflow) ListSceneTilesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	scstr := mux.Vars(req)["scene"]
	scene, err := strconv.Atoi(scstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	ims, err := wf.Tiles(ctx, "", scene, "", false)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.tiles: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(ims)
}

// ListAOITilesHandler lists the tiles of the scene
func (wf *Workflow) ListAOITilesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	aoi := mux.Vars(req)["aoi"]
	status := mux.Vars(req)["status"]
	ims, err := wf.Tiles(ctx, aoi, 0, status, false)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.tiles: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(ims)
}

// RetrySceneHandler retries the scene if its status is RETRY
func (wf *Workflow) RetrySceneHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	sstr := mux.Vars(req)["scene"]
	scene, err := strconv.Atoi(sstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	emptyMessage := ""
	done, err := wf.UpdateSceneStatus(ctx, scene, common.StatusPENDING, &emptyMessage, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.retryscenehandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(ifElse(done, 200, 403))
}

// FailSceneHandler tags the scene and its tiles as FAILED
func (wf *Workflow) FailSceneHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	sstr := mux.Vars(req)["scene"]
	scene, err := strconv.Atoi(sstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	done, err := wf.UpdateSceneStatus(ctx, scene, common.StatusFAILED, nil, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.retryscenehandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(ifElse(done, 200, 403))
}

// ForceSceneStatusHandler set the scene status and updates the graph
func (wf *Workflow) ForceSceneStatusHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	sstr := mux.Vars(req)["scene"]
	status, err := common.StatusString(mux.Vars(req)["status"])
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}

	scene, err := strconv.Atoi(sstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	done, err := wf.UpdateSceneStatus(ctx, scene, status, nil, true)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ForceSceneStatusHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(ifElse(done, 200, 403))
}

// GetTileHandler retrieves the tile
func (wf *Workflow) GetTileHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	bstr := mux.Vars(req)["tile"]
	tile, err := strconv.Atoi(bstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	im, _, err := wf.Tile(ctx, tile, true)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.GetTileHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(im)
}

// RetryTileHandler retries the tile if its status is RETRY
func (wf *Workflow) RetryTileHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	bstr := mux.Vars(req)["tile"]
	tile, err := strconv.Atoi(bstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	emptyMessage := ""
	done, err := wf.UpdateTileStatus(ctx, tile, common.StatusPENDING, &emptyMessage, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.RetryTileHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(ifElse(done, 200, 403))
}

// FailTileHandler tags the tile as FAILED and updates the graph
func (wf *Workflow) FailTileHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	bstr := mux.Vars(req)["tile"]
	tile, err := strconv.Atoi(bstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	done, err := wf.UpdateTileStatus(ctx, tile, common.StatusFAILED, nil, false)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.FailTileHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(ifElse(done, 200, 403))
}

// ForceTileStatusHandler set the tile status and updates the graph
func (wf *Workflow) ForceTileStatusHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	tstr := mux.Vars(req)["tile"]
	status, err := common.StatusString(mux.Vars(req)["status"])
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}

	tile, err := strconv.Atoi(tstr)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	done, err := wf.UpdateTileStatus(ctx, tile, status, nil, true)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ForceTileStatusHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(ifElse(done, 200, 403))
}

// PrintDotHandler returns a xdot-representation of the graph
func (wf *Workflow) PrintDotHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	aoi := mux.Vars(req)["aoi"]
	err := wf.Dot(ctx, aoi, w)
	if err != nil {
		log.Logger(ctx).Error("print dot", zap.Error(err))
	}
}

// ListAOIsHandler returns the list of aois
func (wf *Workflow) ListAOIsHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	pattern := mux.Vars(req)["pattern"]
	aois, err := wf.AOIs(ctx, pattern)
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.aois: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(aois)
}

// GetAOIStatusHandler returns infos on the aoi
func (wf *Workflow) GetAOIStatusHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	aoi := mux.Vars(req)["aoi"]
	scenesStatus, err := wf.ScenesStatus(ctx, aoi)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	tilesStatus, err := wf.TilesStatus(ctx, aoi)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	rootTiles, err := wf.RootTiles(ctx, aoi)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	leafTiles, err := wf.LeafTiles(ctx, aoi)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	from := time.Now()
	to := time.Time{}
	for _, tile := range rootTiles {
		if tile.Scene.Data.Date.Before(from) {
			from = tile.Scene.Data.Date
		}
	}
	for _, tile := range leafTiles {
		if tile.Scene.Data.Date.After(to) {
			to = tile.Scene.Data.Date
		}
	}

	w.WriteHeader(200)
	fmt.Fprintf(w, "Scenes:\n  new:     %d\n  pending: %d\n  done:    %d\n  retry:   %d\n  failed:  %d\n  Total:   %d\n",
		scenesStatus.New, scenesStatus.Pending, scenesStatus.Done, scenesStatus.Retry, scenesStatus.Failed,
		scenesStatus.New+scenesStatus.Pending+scenesStatus.Done+scenesStatus.Retry+scenesStatus.Failed)
	fmt.Fprintf(w, "Tiles:\n  new:     %d\n  pending: %d\n  done:    %d\n  retry:   %d\n  failed:  %d\n  Total:   %d\n",
		tilesStatus.New, tilesStatus.Pending, tilesStatus.Done, tilesStatus.Retry, tilesStatus.Failed,
		tilesStatus.New+tilesStatus.Pending+tilesStatus.Done+tilesStatus.Retry+tilesStatus.Failed)
	fmt.Fprintf(w, "\nRoot tiles : %d\n  From: %s\n  To:   %s\n", len(rootTiles), from.Format("2006-01-02"), to.Format("2006-01-02"))
}

// CreateAOIHandler creates a new aoi
func (wf *Workflow) CreateAOIHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	if err := wf.CreateAOI(ctx, mux.Vars(req)["aoi"]); err != nil {
		if errors.As(err, &db.ErrAlreadyExists{}) {
			w.WriteHeader(409)
			return
		}
		log.Logger(ctx).Sugar().Warnf("create: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(204)
}

// DeleteAOIHandler delete an aoi, its scenes and its tile (cannot be reverted)
func (wf *Workflow) DeleteAOIHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	if err := wf.DeleteAOI(ctx, mux.Vars(req)["aoi"]); err != nil {
		log.Logger(ctx).Sugar().Warnf("delete: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.WriteHeader(204)
}

// CreateSceneHandler creates a new scene
func (wf *Workflow) CreateSceneHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	sc := common.SceneToIngest{}
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&sc)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	nid, err := wf.IngestScene(ctx, mux.Vars(req)["aoi"], sc)
	if err != nil {
		if errors.As(err, &db.ErrAlreadyExists{}) {
			w.WriteHeader(409)
			return
		}
		log.Logger(ctx).Sugar().Warnf("ingest: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	fmt.Fprintf(w, "{\"id\":%d}", nid)
}

// ListScenesHandler lists the scenes of the aoi
// If status is provided, filter only the scenes with the given status
func (wf *Workflow) ListScenesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ss, err := wf.Scenes(ctx, mux.Vars(req)["aoi"])

	status := mux.Vars(req)["status"]
	if status != "" {
		j := 0
		for i := 0; i < len(ss); i++ {
			if ss[i].Status.String() == status {
				ss[j] = ss[i]
				j++
			}
		}
		ss = ss[0:j]
	}

	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.scenes: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(ss)
}

// ListRootTilesHandler lists all the tiles of the AOI that
// have no ref tile (root)
func (wf *Workflow) ListRootTilesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	aoi := mux.Vars(req)["aoi"]
	ims, err := wf.RootTiles(ctx, aoi)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ListRootTilesHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(ims)
}

// ListLeaftTilesHandler lists all the tiles of the AOI that are the previous of no tile (leaf)
func (wf *Workflow) ListLeafTilesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	aoi := mux.Vars(req)["aoi"]
	ims, err := wf.LeafTiles(ctx, aoi)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.ListLeafTilesHandler: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	json.NewEncoder(w).Encode(ims)
}

// RetryAOIHandler retries all the scenes and tiles with the status 'RETRY' (and also 'PENDING' if force=true)
func (wf *Workflow) RetryAOIHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	aoiID := mux.Vars(req)["aoi"]
	ss, err := wf.Scenes(ctx, aoiID)
	if errors.As(err, &db.ErrNotFound{}) {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Logger(ctx).Sugar().Warnf("wf.scenes: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	force := mux.Vars(req)["force"] == "force"
	nbScenes := 0
	emptyMessage := ""
	var errs error

	// Retry scenes
	for _, scene := range ss {
		if force || scene.Status == common.StatusRETRY {
			done, err := wf.UpdateSceneStatus(ctx, scene.ID, common.StatusPENDING, &emptyMessage, force)
			if err != nil {
				errs = service.MergeErrors(false, errs, err)
			} else if done {
				nbScenes++
			}
		}
	}

	// Retry tiles
	nbTiles := 0
	if force {
		// Scan all tiles... It can be very long...
		for _, scene := range ss {
			if scene.Status == common.StatusDONE {
				tiles, err := wf.Tiles(ctx, "", scene.ID, "", false)
				if err != nil {
					errs = service.MergeErrors(false, errs, err)
					continue
				}
				for _, tile := range tiles {
					if force || tile.Status == common.StatusRETRY {
						done, err := wf.UpdateTileStatus(ctx, tile.ID, common.StatusPENDING, &emptyMessage, force)
						if err != nil {
							errs = service.MergeErrors(false, errs, err)
						} else if done {
							nbTiles++
						}
					}
				}
			}
		}
	} else {
		// Only load the tiles with the "RETRY" status
		tiles, err := wf.Tiles(ctx, aoiID, 0, common.StatusRETRY.String(), false)
		if err != nil {
			errs = service.MergeErrors(false, errs, err)
		} else {
			for _, tile := range tiles {
				done, err := wf.UpdateTileStatus(ctx, tile.ID, common.StatusPENDING, &emptyMessage, force)
				if err != nil {
					errs = service.MergeErrors(false, errs, err)
				} else if done {
					nbTiles++
				}
			}
		}
	}
	if errs != nil {
		log.Logger(ctx).Sugar().Warnf("wf.retryaoihandler.%v", errs)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", errs)
		return
	}
	if nbScenes == 0 && nbTiles == 0 {
		w.WriteHeader(204)
	} else {
		json.NewEncoder(w).Encode(struct{ Scenes, Tiles int }{nbScenes, nbTiles})
	}
}
