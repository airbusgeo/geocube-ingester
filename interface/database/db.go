package db

import (
	"context"
	"fmt"

	"github.com/airbusgeo/geocube-ingester/common"
)

type Scene struct {
	common.Scene
	Status  common.Status `json:"status"`
	Message string        `json:"message"`
}

type Tile struct {
	common.Tile
	Status      common.Status `json:"status"`
	Message     string        `json:"message"`
	PreviousID  *int
	ReferenceID *int
}

type ErrAlreadyExists struct {
	Type, ID string
}

func (e ErrAlreadyExists) Error() string {
	return fmt.Sprintf("%s alreay exists: %s", e.Type, e.ID)
}

type ErrNotFound struct {
	Type, ID string
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("%s not found: %s", e.Type, e.ID)
}

type WorkflowTxBackend interface {
	WorkflowBackend
	// Must be call to apply transaction
	Commit() error
	// Might be called to cancel the transaction (no effect if commit has already be done)
	Rollback() error
}

type WorkflowDBBackend interface {
	WorkflowBackend
	StartTransaction(ctx context.Context) (WorkflowTxBackend, error)
}

type Status struct {
	New, Pending, Done, Retry, Failed int64
}

// Set the number of occurences for a given status
func (s *Status) Set(status common.Status, nb int64) {
	switch status {
	case common.StatusNEW:
		s.New = nb
	case common.StatusPENDING:
		s.Pending = nb
	case common.StatusDONE:
		s.Done = nb
	case common.StatusRETRY:
		s.Retry = nb
	case common.StatusFAILED:
		s.Failed = nb
	}
}

type WorkflowBackend interface {
	// Create an AOI in database, may return ErrAlreadyExists
	CreateAOI(ctx context.Context, aoi string) error
	// AOIs returns the list of the aois fitting the pattern
	// pattern [optional=""] aoi_patern
	AOIs(ctx context.Context, pattern string) ([]string, error)
	// Delete an AOI from the database
	DeleteAOI(ctx context.Context, aoi string) error

	// Returns the status of the scenes of the aoi
	ScenesStatus(ctx context.Context, aoi string) (Status, error)
	// Create a new scene, returning its id
	CreateScene(ctx context.Context, sourceID, aoi string, status common.Status, data common.SceneAttrs) (int, error)
	// Get scene with the given id, may return ErrNotFound
	// If a scenesCache is provided, try first to get the scene from the map. Otherwise, the map is updated
	Scene(ctx context.Context, id int, scenesCache *map[int]Scene) (Scene, error)
	// List scenes of the given AOI
	Scenes(ctx context.Context, aoi string, page, limit int) ([]Scene, error)
	// Update scene status & message (if != nil)
	UpdateScene(ctx context.Context, id int, status common.Status, message *string) error
	// Update scene data
	UpdateSceneAttrs(ctx context.Context, id int, data common.SceneAttrs) error
	// Returns the id of a scene. May return ErrNotFound
	SceneId(ctx context.Context, aoi, sourceID string) (int, error)

	// Returns the status of the tiles of the aoi
	TilesStatus(ctx context.Context, aoi string) (Status, error)
	// Create a new tile, returning its id
	// prevTileSource == "" || refTileSource == "" => root tile
	CreateTile(ctx context.Context, sourceID string, sceneID int, data common.TileAttrs, aoi, prevTileSource, prevSceneSource, refTileSource, refSceneSource string) (int, error)
	// Get tile with the given id and status of the scene. May return ErrNotFound
	// If loadScene, the scene is also loaded
	Tile(ctx context.Context, id int, loadScene bool) (Tile, common.Status, error)
	// Tiles returns the list of tiles fitting the given parameters
	// aoi [optional=""] aoi
	// sceneID [optional=0] sceneID
	// status [optional=""] status of the tile
	// loadScene also loads the scenes
	Tiles(ctx context.Context, aoi string, sceneID int, status string, loadScene bool, page, limit int) ([]Tile, error)
	// Get root tiles (no prev and no ref tiles) and their scene.
	RootTiles(ctx context.Context, aoi string) ([]common.Tile, error)
	// Get leaf tiles (no next tiles) and their scene.
	LeafTiles(ctx context.Context, aoi string) ([]common.Tile, error)
	// Update tile status & message (if != nil)
	UpdateTile(ctx context.Context, id int, status common.Status, message *string, resetPrev bool) error
	// Set status of given tiles
	SetTilesStatus(ctx context.Context, ids []int, status common.Status) error
	// Update status of tiles given previous tile ID, current status and scene status
	// Returns updated tiles and their scenesID
	UpdateNextTilesStatus(ctx context.Context, prevID int, status, sceneStatus, newStatus common.Status) ([]Tile, []int, error)
	// Update status of tiles given scene ID, current status and status of previous tile
	// Returns updated tiles, previous tiles and previous scenes ID
	UpdateSceneTilesStatus(ctx context.Context, sceneID int, status, prevStatus, newStatus common.Status) ([]Tile, []Tile, []int, error)
	// Same as UpdateSceneTilesStatus, but only updates root tiles
	UpdateSceneRootTilesStatus(ctx context.Context, sceneID int, status, newStatus common.Status) ([]Tile, error)
	// Update next tile, setting newPrevID
	// Returns list of modified tiles
	UpdateNextTilesPrevId(ctx context.Context, oldPrevID int, newPrevID *int) ([]int, error)
	// Update ref tile, setting newRefID
	// Returns list of modified tiles
	UpdateRefTiles(ctx context.Context, oldRefID int, newRefID int) error
	// Update tile data
	UpdateTileAttrs(ctx context.Context, id int, data common.TileAttrs) error
}

// UnitOfWork runs a function and commit the database at the end or rollback if the function returns an error
func UnitOfWork(ctx context.Context, db WorkflowDBBackend, f func(tx WorkflowTxBackend) error) (err error) {
	// Start transaction
	txn, err := db.StartTransaction(ctx)
	if err != nil {
		return fmt.Errorf("uow.starttransaction: %w", err)
	}

	// Rollback if not successful
	defer func() {
		if e := txn.Rollback(); err == nil {
			err = e
		}
	}()

	// Execute function
	if err = f(txn); err != nil {
		return fmt.Errorf("uow.%w", err)
	}

	return txn.Commit()
}
