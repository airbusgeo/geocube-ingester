package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/airbusgeo/geocube-ingester/catalog"
	"github.com/airbusgeo/geocube-ingester/common"
	db "github.com/airbusgeo/geocube-ingester/interface/database"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube/interface/messaging"
)

type Workflow struct {
	db.WorkflowDBBackend
	dbmu       sync.Mutex
	sceneQueue messaging.Publisher
	tileQueue  messaging.Publisher

	catalog *catalog.Catalog
}

func NewWorkflow(db db.WorkflowDBBackend, sceneQueue, tileQueue messaging.Publisher, catalog *catalog.Catalog) *Workflow {
	return &Workflow{
		WorkflowDBBackend: db,
		sceneQueue:        sceneQueue,
		tileQueue:         tileQueue,
		catalog:           catalog,
	}
}

func (wf *Workflow) FailTile(ctx context.Context, tile db.Tile, tx db.WorkflowTxBackend) error {
	lg := log.Logger(ctx).Sugar()
	var ptile db.Tile
	var err error
	if tile.PreviousID != nil {
		var sceneStatus common.Status
		ptile, sceneStatus, err = wf.Tile(ctx, *tile.PreviousID, true)
		if err != nil {
			return fmt.Errorf("get previous tile: %w", err)
		}
		if ptile.Status == common.StatusFAILED {
			return fmt.Errorf("previous tile is %s", ptile.Status)
		}
		if sceneStatus == common.StatusFAILED {
			return fmt.Errorf("previous scene is %s", sceneStatus)
		}
	}

	// Update tile
	if err := tx.UpdateTile(ctx, tile.ID, common.StatusFAILED, &tile.Message, true); err != nil {
		return fmt.Errorf("FailTile.%w", err)
	}

	//update previous of dependants
	bids, err := tx.UpdateNextTilesPrevId(ctx, tile.ID, tile.PreviousID)
	if err != nil {
		return fmt.Errorf("FailTile.%w", err)
	}
	// If root tile, update reference
	if tile.ReferenceID == nil && len(bids) != 0 {
		if err = tx.UpdateRefTiles(ctx, tile.ID, bids[0]); err != nil {
			return fmt.Errorf("FailTile.%w", err)
		}
	}
	var publishes [][]byte
	idsToMarkPending := []int{}
	for _, bid := range bids {
		ctile, sceneStatus, err := tx.Tile(ctx, bid, true)
		if err != nil {
			return fmt.Errorf("get tile %d: %w", bid, err)
		}
		if ctile.Status != common.StatusNEW {
			return fmt.Errorf("child tile %d status %s", bid, ctile.Status)
		}
		if sceneStatus == common.StatusDONE {
			idsToMarkPending = append(idsToMarkPending, ctile.ID)
			lg.Infof("queueing tile %s/%s", ctile.Scene.SourceID, ctile.SourceID)
			prepublish, err := wf.prepublishTile(ctx, ctile.Tile, &ptile.Tile, ctile.ReferenceID)
			if err != nil {
				return fmt.Errorf("FailTile.%w", err)
			}
			publishes = append(publishes, prepublish)
		}
	}
	if err = tx.SetTilesStatus(ctx, idsToMarkPending, common.StatusPENDING); err != nil {
		return fmt.Errorf("FailTile[%v].%w", idsToMarkPending, err)
	}
	// Publish
	if err = wf.tileQueue.Publish(ctx, publishes...); err != nil {
		return fmt.Errorf("FailTile: failed to publish: %w", err)
	}
	return nil
}

func (wf *Workflow) RetryTile(ctx context.Context, tile db.Tile) error {
	var ptile db.Tile
	if tile.PreviousID != nil {
		var err error
		if ptile, _, err = wf.Tile(ctx, *tile.PreviousID, true); err != nil {
			return fmt.Errorf("get previous tile: %w", err)
		}
		if ptile.Status != common.StatusDONE {
			return fmt.Errorf("cannot retry tile when previous is %s", ptile.Status)
		}
	}

	lg := log.Logger(ctx).Sugar()
	err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		if err := tx.UpdateTile(ctx, tile.ID, common.StatusPENDING, nil, false); err != nil {
			return err
		}
		lg.Infof("retrying tile %s/%s", tile.Scene.SourceID, tile.SourceID)
		publish, err := wf.prepublishTile(ctx, tile.Tile, &ptile.Tile, tile.ReferenceID)
		if err != nil {
			return err
		}
		return wf.tileQueue.Publish(ctx, publish)
	})

	if err != nil {
		return fmt.Errorf("RetryTile.%w", err)
	}
	return nil
}

func (wf *Workflow) FinishTile(ctx context.Context, tile db.Tile) error {
	lg := log.Logger(ctx).Sugar()
	scenes := map[int]db.Scene{
		tile.Scene.ID: {Scene: tile.Scene},
	}

	err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		var publishes [][]byte
		if err := tx.UpdateTile(ctx, tile.ID, common.StatusDONE, nil, false); err != nil {
			return err
		}
		// Update next tiles
		nextTiles, scenesID, err := tx.UpdateNextTilesStatus(ctx, tile.ID, common.StatusNEW, common.StatusDONE, common.StatusPENDING)
		if err != nil {
			return err
		}
		// Start next tiles
		for i, nextTile := range nextTiles {
			sc, err := tx.Scene(ctx, scenesID[i], &scenes)
			if err != nil {
				return fmt.Errorf("Tile[%d]Scene[%d].%w", nextTile.ID, scenesID[i], err)
			}
			nextTile.Scene = sc.Scene
			lg.Infof("queueing tile %s/%s", nextTile.Scene.SourceID, nextTile.SourceID)
			prepublish, err := wf.prepublishTile(ctx, nextTile.Tile, &tile.Tile, nextTile.ReferenceID)
			if err != nil {
				return err
			}
			publishes = append(publishes, prepublish)
		}
		// Publish
		return wf.tileQueue.Publish(ctx, publishes...)
	})
	if err != nil {
		return fmt.Errorf("FinishTile.%w", err)
	}

	return nil
}

func (wf *Workflow) UpdateTileStatus(ctx context.Context, id int, status common.Status, message *string, force bool) (bool, error) {
	lg := log.Logger(ctx).Sugar()
	wf.dbmu.Lock()
	defer wf.dbmu.Unlock()

	tile, sceneStatus, err := wf.Tile(ctx, id, true)
	if err != nil {
		if errors.As(err, &db.ErrNotFound{}) {
			lg.Errorf("update: %v", err)
			return false, nil
		}
		return false, fmt.Errorf("UpdateTileStatus: %w", err)
	}
	if message != nil {
		tile.Message = *message
	}

	lg.Infof("update tile status %s/%s: %s->%s (%s)", tile.Scene.SourceID, tile.SourceID, tile.Status, status, tile.Message)

	if force {
		switch status {
		case common.StatusDONE:
			tile.Status = common.StatusDONE
			err = wf.FinishTile(ctx, tile)
		case common.StatusRETRY, common.StatusNEW:
			tile.Status = status
			err = wf.UpdateTile(ctx, id, status, &tile.Message, false)
		case common.StatusPENDING:
			tile.Status = common.StatusPENDING
			err = wf.RetryTile(ctx, tile)
		case common.StatusFAILED:
			tile.Status = common.StatusFAILED
			err = db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
				return wf.FailTile(ctx, tile, tx)
			})
		}
		if err != nil {
			return false, err
		}

		err = wf.updateAOIStatus(ctx, wf, tile.Scene.AOI, status == common.StatusRETRY)
		return true, err
	}

	if tile.Status == status {
		lg.Warnf("update tile %d: status already %s", id, status)
		return false, nil
	}

	switch tile.Status {
	case common.StatusPENDING:
		switch status {
		case common.StatusDONE:
			tile.Status = common.StatusDONE
			err = wf.FinishTile(ctx, tile)
		case common.StatusRETRY:
			if tile.RetryCountDown > 0 {
				tile.Status = common.StatusPENDING
				err = wf.RetryTile(ctx, tile)
				status = common.StatusPENDING
			} else {
				tile.Status = common.StatusRETRY
				if err := wf.UpdateTile(ctx, id, common.StatusRETRY, &tile.Message, false); err != nil {
					return false, fmt.Errorf("update retry/fail status: %w", err)
				}
			}
		case common.StatusFAILED:
			tile.Status = common.StatusFAILED
			err = db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
				return wf.FailTile(ctx, tile, tx)
			})
		default:
			log.Logger(ctx).Sugar().Errorf("cannot update tile %d status %s->%s", id, tile.Status, status)
			return false, nil
		}
	case common.StatusRETRY:
		switch status {
		case common.StatusDONE:
			tile.Status = common.StatusDONE
			err = wf.FinishTile(ctx, tile)
		case common.StatusPENDING:
			//sanity check
			if sceneStatus != common.StatusDONE {
				return false, fmt.Errorf("cannot retry tile with scene status %s", sceneStatus)
			}
			tile.Status = common.StatusPENDING
			err = wf.RetryTile(ctx, tile)
		case common.StatusFAILED:
			tile.Status = common.StatusFAILED
			err = db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
				return wf.FailTile(ctx, tile, tx)
			})
		default:
			lg.Errorf("cannot update tile %d status %s->%s", id, tile.Status, status)
			return false, nil
		}
	default:
		lg.Errorf("cannot update tile %d status %s->%s", id, tile.Status, status)
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err = wf.updateAOIStatus(ctx, wf, tile.Scene.AOI, status == common.StatusRETRY); err != nil {
		return true, err
	}

	return true, nil
}

func (wf *Workflow) Dot(ctx context.Context, aoi string, out io.Writer) error {
	fmt.Fprintf(out, "digraph %s {\n", aoi)
	defer fmt.Fprintf(out, "}\n")
	scenes, err := wf.Scenes(ctx, aoi, 0, 1000)
	if err != nil {
		return err
	}
	sort.Slice(scenes, func(i, j int) bool { return scenes[i].ID < scenes[j].ID })
	for _, sc := range scenes {
		fmt.Fprintf(out, "s%d [label=\"%s\\n(id=%d)\" shape=box color=%s];\n", sc.ID, sc.SourceID, sc.ID, sc.Status.Color())
	}
	for _, sc := range scenes {
		tiles, err := wf.Tiles(ctx, "", sc.ID, "", false, 0, -1)
		if err != nil {
			return err
		}
		sort.Slice(tiles, func(i, j int) bool { return tiles[i].ID < tiles[j].ID })
		for _, tile := range tiles {
			fmt.Fprintf(out, "t%d [label=\"%s\\n(id=%d)\" color=%s];\n", tile.ID, tile.SourceID, tile.ID, tile.Status.Color())
		}
	}
	for _, sc := range scenes {
		tiles, err := wf.Tiles(ctx, "", sc.ID, "", false, 0, -1)
		if err != nil {
			return err
		}
		istyle := ""
		if sc.Status != common.StatusDONE {
			istyle = " style=dotted"
		}
		for _, tile := range tiles {
			tstyle := ""
			if tile.Status != common.StatusDONE {
				tstyle = " style=dotted"
			}
			fmt.Fprintf(out, "s%d -> t%d [color=gray%s];\n", sc.ID, tile.ID, istyle)
			if tile.PreviousID != nil {
				fmt.Fprintf(out, "t%d -> t%d%s;\n", *tile.PreviousID, tile.ID, tstyle)
			}
		}
	}
	return nil
}

func (wf *Workflow) FinishScene(ctx context.Context, scene db.Scene) error {
	lg := log.Logger(ctx).Sugar()
	scenes := map[int]db.Scene{scene.ID: scene}
	var publishes [][]byte
	scene.Status = common.StatusDONE

	err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		// Update scene status
		if err := tx.UpdateScene(ctx, scene.ID, common.StatusDONE, nil); err != nil {
			return err
		}

		// Publish root tiles of the scene
		tiles, err := tx.UpdateSceneRootTilesStatus(ctx, scene.ID, common.StatusNEW, common.StatusPENDING)
		if err != nil {
			return err
		}
		for _, tile := range tiles {
			tile.Scene = scene.Scene
			lg.Infof("queueing root tile %s/%s", tile.Scene.SourceID, tile.SourceID)
			prepublish, err := wf.prepublishTile(ctx, tile.Tile, nil, nil)
			if err != nil {
				return err
			}
			publishes = append(publishes, prepublish)
		}

		// Publish other tiles if prev tile is done
		ctiles, ptiles, pscenesID, err := tx.UpdateSceneTilesStatus(ctx, scene.ID, common.StatusNEW, common.StatusDONE, common.StatusPENDING)
		if err != nil {
			return err
		}
		for i, ctile := range ctiles {
			ctile.Scene = scene.Scene
			sc, err := tx.Scene(ctx, pscenesID[i], &scenes)
			if err != nil {
				return fmt.Errorf("get tile %d scene %d: %w", ctile.ID, pscenesID[i], err)
			}
			ptiles[i].Scene = sc.Scene

			lg.Infof("queueing tile %s/%s (parent %s/%s)", ctile.Scene.SourceID, ctile.SourceID, ptiles[i].Scene.SourceID, ptiles[i].SourceID)
			prepublish, err := wf.prepublishTile(ctx, ctile.Tile, &ptiles[i].Tile, ctile.ReferenceID)
			if err != nil {
				return err
			}
			publishes = append(publishes, prepublish)
		}
		//Publish
		return wf.tileQueue.Publish(ctx, publishes...)
	})
	if err != nil {
		return fmt.Errorf("FinishScene.%w", err)
	}
	return nil
}

func (wf *Workflow) RetryScene(ctx context.Context, scene db.Scene) error {
	lg := log.Logger(ctx).Sugar()
	err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		if err := tx.UpdateScene(ctx, scene.ID, common.StatusPENDING, nil); err != nil {
			return err
		}
		lg.Infof("retrying scene %s", scene.SourceID)
		return wf.publishScene(ctx, scene.Scene)
	})
	if err != nil {
		return fmt.Errorf("RetryScene.%w", err)
	}
	return nil
}

func (wf *Workflow) FailScene(ctx context.Context, scene db.Scene) error {
	tiles, err := wf.Tiles(ctx, "", scene.ID, "", false, 0, -1)
	if err != nil {
		return fmt.Errorf("get tiles: %w", err)
	}
	//sanity check
	for _, tile := range tiles {
		if tile.Status != common.StatusNEW {
			return fmt.Errorf("unconsistency: tile %d status %s", tile.ID, tile.Status)
		}
	}

	err = db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		if err := tx.UpdateScene(ctx, scene.ID, common.StatusFAILED, &scene.Message); err != nil {
			return err
		}
		for _, tile := range tiles {
			if err = wf.FailTile(ctx, tile, tx); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("FailScene.%w", err)
	}
	return nil
}

func (wf *Workflow) UpdateSceneStatus(ctx context.Context, id int, status common.Status, message *string, force bool) (bool, error) {
	lg := log.Logger(ctx).Sugar()
	wf.dbmu.Lock()
	defer wf.dbmu.Unlock()

	scene, err := wf.Scene(ctx, id, nil)
	if errors.As(err, &db.ErrNotFound{}) {
		lg.Errorf("update: %v", err)
		return false, nil
	}
	if message != nil {
		scene.Message = *message
	}

	lg.Infof("update scene status %s: %s->%s (%s)", scene.SourceID, scene.Status, status, scene.Message)

	if force {
		switch status {
		case common.StatusDONE:
			err = wf.FinishScene(ctx, scene)
		case common.StatusRETRY, common.StatusNEW:
			err = wf.UpdateScene(ctx, id, status, &scene.Message)
		case common.StatusFAILED:
			err = wf.FailScene(ctx, scene)
		case common.StatusPENDING:
			err = wf.RetryScene(ctx, scene)
		}
		if err != nil {
			return true, err
		}
		err = wf.updateAOIStatus(ctx, wf, scene.AOI, status == common.StatusRETRY)
		return true, err
	}

	if scene.Status == status {
		lg.Warnf("update scene %d: status already %s", id, status)
		return false, nil
	}

	switch scene.Status {
	case common.StatusPENDING:
		switch status {
		case common.StatusDONE:
			err = wf.FinishScene(ctx, scene)
		case common.StatusRETRY:
			if scene.RetryCountDown > 0 {
				err = wf.RetryScene(ctx, scene)
				status = common.StatusPENDING
			} else {
				err = wf.UpdateScene(ctx, id, status, &scene.Message)
			}
		case common.StatusFAILED:
			err = wf.FailScene(ctx, scene)
		default:
			lg.Errorf("cannot update scene %d status %s->%s", id, scene.Status, status)
			return false, nil
		}
	case common.StatusRETRY:
		switch status {
		case common.StatusDONE:
			err = wf.FinishScene(ctx, scene)
		case common.StatusPENDING:
			err = wf.RetryScene(ctx, scene)
		case common.StatusFAILED:
			err = wf.FailScene(ctx, scene)
		default:
			lg.Errorf("cannot update scene %d status %s->%s", id, scene.Status, status)
			return false, nil
		}
	default:
		lg.Errorf("cannot update scene %d status %s->%s", id, scene.Status, status)
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := wf.updateAOIStatus(ctx, wf, scene.AOI, status == common.StatusRETRY); err != nil {
		return true, err
	}

	return true, err
}

// IngestScene adds a new scene to the workflow and starts the processing
// Return id of the scene
func (wf *Workflow) IngestScene(ctx context.Context, aoi string, scene common.SceneToIngest) (int, error) {
	wf.dbmu.Lock()
	defer wf.dbmu.Unlock()

	if scene.ID != 0 {
		return 0, fmt.Errorf("ingestScene: scene must not have id set")
	}
	if len(scene.Tiles) == 0 || len(scene.Tiles) != len(scene.Data.TileMappings) {
		return 0, fmt.Errorf("ingestScene: scene has no tiles")
	}
	if scene.AOI != aoi {
		return 0, fmt.Errorf("ingestScene: scene.AOI and aoi are different")
	}

	// Check that the scene does not already exists
	if _, err := wf.SceneId(ctx, aoi, scene.SourceID); err != nil && !errors.As(err, &db.ErrNotFound{}) {
		return 0, fmt.Errorf("query scene: %w", err)
	} else if err == nil {
		return 0, db.ErrAlreadyExists{Type: "scene", ID: scene.SourceID}
	}

	err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		var err error
		if scene.ID == 0 {
			if scene.ID, err = tx.CreateScene(ctx, scene.SourceID, aoi, common.StatusPENDING, scene.Data, scene.RetryCount); err != nil {
				return err
			}
		} else if err := tx.UpdateSceneAttrs(ctx, scene.ID, scene.Data); err != nil {
			return err
		}
		for sourceID, tile := range scene.Tiles {
			if _, err := tx.CreateTile(ctx, sourceID, scene.ID, tile.Data, aoi, tile.PreviousTileID, tile.PreviousSceneID, tile.ReferenceTileID, tile.ReferenceSceneID, scene.RetryCount); err != nil {
				return err
			}
		}

		if err := wf.updateAOIStatus(ctx, tx, aoi, false); err != nil {
			return err
		}
		log.Logger(ctx).Sugar().Infof("queueing image %s", scene.SourceID)
		return wf.publishScene(ctx, scene.Scene)
	})
	if err != nil {
		return 0, fmt.Errorf("IngestScene.%w", err)
	}

	return scene.ID, nil
}

// UpdateSceneData update the data of a scene
func (wf *Workflow) UpdateSceneData(ctx context.Context, sceneID int, data common.SceneAttrs) error {
	wf.dbmu.Lock()
	defer wf.dbmu.Unlock()
	if err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		return tx.UpdateSceneAttrs(ctx, sceneID, data)
	}); err != nil {
		return fmt.Errorf("UpdateSceneData.%w", err)
	}

	return nil
}

// UpdateTileData update the data of a tile
func (wf *Workflow) UpdateTileData(ctx context.Context, tileID int, data common.TileAttrs) error {
	wf.dbmu.Lock()
	defer wf.dbmu.Unlock()
	if err := db.UnitOfWork(ctx, wf, func(tx db.WorkflowTxBackend) error {
		return tx.UpdateTileAttrs(ctx, tileID, data)
	}); err != nil {
		return fmt.Errorf("UpdateTileData.%w", err)
	}

	return nil
}

func (wf *Workflow) publishScene(ctx context.Context, scene common.Scene) error {
	plb, err := json.Marshal(scene)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err = wf.sceneQueue.Publish(ctx, plb); err != nil {
		return fmt.Errorf("failed to enqueue: %w", err)
	}
	return nil
}

func (wf *Workflow) prepublishTile(ctx context.Context, tile common.Tile, prevTile *common.Tile, refID *int) ([]byte, error) {
	tileToProcess := common.TileToProcess{
		Tile: tile,
	}

	// Define previous tile
	if prevTile != nil {
		tileToProcess.Previous = *prevTile
	}

	// Load reference tile
	if refID != nil {
		tile, _, err := wf.Tile(ctx, *refID, true)
		if err != nil {
			return nil, fmt.Errorf("prepublishTile.%w", err)
		}
		tileToProcess.Reference = tile.Tile
	}

	// Marshal
	plb, err := json.Marshal(tileToProcess)
	if err != nil {
		return nil, fmt.Errorf("prepublishTile.Message: %w", err)
	}
	return plb, nil
}

func (wf *Workflow) updateAOIStatus(ctx context.Context, wfb db.WorkflowBackend, aoi string, isRetry bool) error {
	_, _, err := wfb.UpdateAOIStatus(ctx, aoi, isRetry)
	return err
}
