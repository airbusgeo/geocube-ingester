package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/airbusgeo/geocube-ingester/common"
	db "github.com/airbusgeo/geocube-ingester/interface/database"
	"github.com/lib/pq"
)

// pgInterface allows to use either a sql.DB or a sql.Tx
type pgInterface interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// BackendTx implements WorkflowTxBackend
type BackendTx struct {
	*sql.Tx
	Backend
}

// BackendDB implements WorkflowDBBackend
type BackendDB struct {
	*sql.DB
	Backend
}

// Backend implements WorkflowBackend
type Backend struct {
	pgInterface
}

/* http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html */
const (
	noError             = "00000"
	connectionFailure   = "08006"
	foreignKeyViolation = "23503"
	uniqueViolation     = "23505"

	notPqError = "X"
)

func pqErrorCode(err error) pq.ErrorCode {
	if err == nil {
		return noError
	}
	var pqerr *pq.Error
	if errors.As(err, &pqerr) {
		return pqerr.Code
	}
	return notPqError
}

// StartTransaction implements WorkflowDBBackend
func (bdb BackendDB) StartTransaction(ctx context.Context) (db.WorkflowTxBackend, error) {
	tx, err := bdb.BeginTx(ctx, nil)
	if err != nil {
		return BackendTx{}, err
	}
	return BackendTx{tx, Backend{pgInterface: tx}}, nil
}

// Rollback overloads sql.Tx.Rollback to be idempotent
func (btx BackendTx) Rollback() error {
	err := btx.Tx.Rollback()
	if err == sql.ErrTxDone {
		return nil
	}
	return err
}

// New creates a new backend using Postgres
func New(ctx context.Context, dbConnection string) (*BackendDB, error) {
	db, err := sql.Open("postgres", dbConnection)
	if err != nil {
		return nil, fmt.Errorf("sql.open: %w", err)
	}
	return &BackendDB{db, Backend{pgInterface: db}}, nil
}

// AOIs implements WorkflowBackend
func (b Backend) AOIs(ctx context.Context, aoi string) ([]string, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if aoi == "" {
		rows, err = b.QueryContext(ctx, "select id from aoi ORDER BY id")
	} else {
		rows, err = b.QueryContext(ctx, "select id from aoi where id LIKE $1 ORDER BY id", aoi)
	}

	if err != nil {
		return nil, fmt.Errorf("aois.QueryContext: %w", err)
	}
	defer rows.Close()
	aois := make([]string, 0)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("aois.Scan: %w", err)
		}
		aois = append(aois, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("aois.rows.err: %w", err)
	}
	return aois, nil
}

// CreateAOI implements WorkflowBackend
func (b Backend) CreateAOI(ctx context.Context, aoi string) error {
	_, err := b.ExecContext(ctx, "insert into aoi(id) values($1)", aoi)
	switch pqErrorCode(err) {
	case noError:
		return nil
	case uniqueViolation:
		return db.ErrAlreadyExists{Type: "aoi", ID: aoi}
	default:
		return fmt.Errorf("CreateAOI.exec: %w", err)
	}
}

// ScenesStatus implements WorkflowBackend
func (b Backend) ScenesStatus(ctx context.Context, aoi string) (db.Status, error) {
	s := db.Status{}
	rows, err := b.QueryContext(ctx, "select status, count(status) from scene where aoi_id=$1 group by status", aoi)
	if err != nil {
		return s, fmt.Errorf("ScenesStatus.QueryContext: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status common.Status
		var nb int64
		if err := rows.Scan(&status, &nb); err != nil {
			return s, fmt.Errorf("ScenesStatus.Scan: %w", err)
		}
		s.Set(status, nb)
	}
	if err := rows.Err(); err != nil {
		return s, fmt.Errorf("ScenesStatus.rows.err: %w", err)
	}
	return s, nil
}

// CreateScene implements WorkflowBackend
func (b Backend) CreateScene(ctx context.Context, sourceID, aoi string, status common.Status, data common.SceneAttrs) (int, error) {
	scID := 0
	if err := b.QueryRowContext(ctx, "insert into scene(source_id,aoi_id,status,data) values($1,$2,$3,$4) returning id",
		sourceID, aoi, status, data).Scan(&scID); err != nil {
		return 0, fmt.Errorf("CreateScene: %w", err)
	}
	return scID, nil
}

// Scene implements WorkflowBackend
func (b Backend) Scene(ctx context.Context, id int, scenesCache *map[int]db.Scene) (db.Scene, error) {
	if scenesCache != nil {
		if scene, ok := (*scenesCache)[id]; ok {
			return scene, nil
		}
	}

	scene := db.Scene{}
	scene.ID = id
	if err := b.QueryRowContext(ctx, "select source_id,aoi_id,status,message,data from scene where id=$1", id).Scan(
		&scene.SourceID, &scene.AOI, &scene.Status, &scene.Message, &scene.Data); err != nil {
		if err == sql.ErrNoRows {
			return scene, db.ErrNotFound{Type: "scene", ID: fmt.Sprintf("%d", id)}
		}
		return scene, fmt.Errorf("Scene.QueryRowContext: %w", err)
	}

	if scenesCache != nil {
		(*scenesCache)[id] = scene
	}

	return scene, nil
}

// Scenes implements WorkflowBackend
func (b Backend) Scenes(ctx context.Context, aoi string) ([]db.Scene, error) {
	scenes := make([]db.Scene, 0)
	rows, err := b.QueryContext(ctx, "select id,source_id,status,message,data from scene where aoi_id=$1", aoi)
	if err != nil {
		return nil, fmt.Errorf("scenes.QueryContext: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		s := db.Scene{}
		s.AOI = aoi
		if err := rows.Scan(&s.ID, &s.SourceID, &s.Status, &s.Message, &s.Data); err != nil {
			return nil, fmt.Errorf("scenes.Scan: %w", err)
		}
		scenes = append(scenes, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scenes.rows.err: %w", err)
	}
	return scenes, nil
}

// UpdateScene implements WorkflowBackend
func (b Backend) UpdateScene(ctx context.Context, id int, status common.Status, message *string) error {
	var err error
	if message != nil {
		_, err = b.ExecContext(ctx, "update scene set status=$1, message=$2 where id=$3", status, *message, id)
	} else {
		_, err = b.ExecContext(ctx, "update scene set status=$1 where id=$2", status, id)
	}
	if err != nil {
		return fmt.Errorf("updateScene: %w", err)
	}
	return nil
}

// SceneExists implements WorkflowBackend
func (b Backend) SceneExists(ctx context.Context, aoi, sourceID string) (bool, error) {
	count := 0
	if err := b.QueryRowContext(ctx, "select count(*) from scene where aoi_id=$1 and source_id=$2", aoi, sourceID).Scan(&count); err != nil {
		return false, fmt.Errorf("SceneExists.QueryRowContext: %w", err)
	}
	return count != 0, nil
}

// TilesStatus implements WorkflowBackend
func (b Backend) TilesStatus(ctx context.Context, aoi string) (db.Status, error) {
	s := db.Status{}
	rows, err := b.QueryContext(ctx, "select tile.status, count(tile.status) from tile join scene on tile.scene_id = scene.id where scene.aoi_id=$1 group by tile.status", aoi)
	if err != nil {
		return s, fmt.Errorf("TilesStatus.QueryContext: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status common.Status
		var nb int64
		if err := rows.Scan(&status, &nb); err != nil {
			return s, fmt.Errorf("TilesStatus.Scan: %w", err)
		}
		s.Set(status, nb)
	}
	if err := rows.Err(); err != nil {
		return s, fmt.Errorf("TilesStatus.rows.err: %w", err)
	}
	return s, nil
}

// CreateTile implements WorkflowBackend
func (b Backend) CreateTile(ctx context.Context, sourceID string, sceneID int, data common.TileAttrs, aoi, prevTileSource, prevSceneSource, refTileSource, refSceneSource string) (int, error) {
	bid := 0
	if prevTileSource != "" && refTileSource != "" {
		if err := b.QueryRowContext(ctx, `insert into tile(source_id,scene_id,status,data,prev,ref)
		select $1,$2,$3,$4,tprev.id,tref.id from tile tprev, scene sprev, tile tref, scene sref
		where sprev.aoi_id=$5 and tprev.source_id=$6 and sprev.source_id=$7 and tprev.scene_id=sprev.id and tprev.status != $10
		and   sref.aoi_id =$5 and tref.source_id =$8 and sref.source_id =$9 and tref.scene_id =sref.id  and tref.status  != $10
		RETURNING tile.id`,
			sourceID, sceneID, common.StatusNEW, data, aoi, prevTileSource, prevSceneSource, refTileSource, refSceneSource, common.StatusFAILED).Scan(&bid); err != nil {
			return 0, fmt.Errorf("CreateTile: insert tile %s (parent %s, ref %s) (hint: check parent tile is not FAILED): %w", sourceID, prevSceneSource, refSceneSource, err)
		}
	} else if err := b.QueryRowContext(ctx, "insert into tile(source_id,scene_id,status,data) values($1,$2,$3,$4) RETURNING tile.id", sourceID, sceneID, common.StatusNEW, data).Scan(&bid); err != nil {
		return 0, fmt.Errorf("CreateTile: insert root tile %s: %w", sourceID, err)
	}
	return bid, nil
}

// Tile implements WorkflowBackend
func (b Backend) Tile(ctx context.Context, tile int, loadScene bool) (db.Tile, common.Status, error) {
	ti := db.Tile{}
	ti.ID = tile
	var err error
	var sceneStatus common.Status
	if loadScene {
		err = b.QueryRowContext(ctx,
			"select t.source_id,t.scene_id,t.prev,t.ref,t.status,t.message,t.data, s.source_id,s.aoi_id,s.status,s.data from tile t, scene s where t.id=$1 and t.scene_id = s.id", tile).Scan(
			&ti.SourceID, &ti.Scene.ID, &ti.PreviousID, &ti.ReferenceID, &ti.Status, &ti.Message, &ti.Data, &ti.Scene.SourceID, &ti.Scene.AOI, &sceneStatus, &ti.Scene.Data)
	} else {
		err = b.QueryRowContext(ctx, "select source_id,scene_id,prev,ref,status,message,data from tile where tile.id=$1", tile).Scan(
			&ti.SourceID, &ti.Scene.ID, &ti.PreviousID, &ti.ReferenceID, &ti.Status, &ti.Message, &ti.Data)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return ti, sceneStatus, db.ErrNotFound{Type: "tile", ID: fmt.Sprintf("%d", tile)}
		}
		return ti, sceneStatus, fmt.Errorf("Tile.Scan: %w", err)
	}
	return ti, sceneStatus, nil
}

// Tiles implements WorkflowBackend
func (b Backend) Tiles(ctx context.Context, aoi string, sceneID int, status string, loadScene bool) ([]db.Tile, error) {
	// Construct the query
	query := "select t.id, t.source_id, t.scene_id, t.prev, t.ref, t.status, t.message, t.data"

	if loadScene {
		query += ", s.source_id, s.aoi_id, s.data"
	}
	query += " from tile t"

	if aoi != "" || loadScene {
		query += " JOIN scene s ON s.id = t.scene_id"
	}

	var parameters []interface{}
	var whereClause []string
	if aoi != "" {
		parameters = append(parameters, aoi)
		whereClause = append(whereClause, fmt.Sprintf(" s.aoi_id = $%d", len(parameters)))
	}
	if sceneID != 0 {
		parameters = append(parameters, sceneID)
		whereClause = append(whereClause, fmt.Sprintf(" t.scene_id = $%d", len(parameters)))
	}
	if status != "" {
		parameters = append(parameters, status)
		whereClause = append(whereClause, fmt.Sprintf(" t.status = $%d", len(parameters)))
	}

	// Append the whereClause to the query
	query += " WHERE" + strings.Join(whereClause, " AND")

	tiles := []db.Tile{}
	{
		rows, err := b.QueryContext(ctx, query, parameters...)
		if err != nil {
			return nil, fmt.Errorf("Tiles.QueryContext: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			tile := db.Tile{}
			if loadScene {
				err = rows.Scan(&tile.ID, &tile.SourceID, &tile.Scene.ID, &tile.PreviousID, &tile.ReferenceID, &tile.Status, &tile.Message, &tile.Data, &tile.Scene.SourceID, &tile.Scene.AOI, &tile.Scene.Data)
			} else {
				err = rows.Scan(&tile.ID, &tile.SourceID, &tile.Scene.ID, &tile.PreviousID, &tile.ReferenceID, &tile.Status, &tile.Message, &tile.Data)
			}
			if err != nil {
				return nil, fmt.Errorf("Tiles.Scan: %w", err)
			}
			tiles = append(tiles, tile)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("Tiles.Rows.err: %w", err)
		}
	}
	return tiles, nil
}

// RootTiles implements WorkflowBackend
func (b Backend) RootTiles(ctx context.Context, aoi string) ([]common.Tile, error) {
	tiles := []common.Tile{}
	rows, err := b.QueryContext(ctx,
		`select t.id, t.source_id, t.data, s.id, s.source_id, s.data
			from tile t join scene s on t.scene_id = s.id
			where t.status != $1 AND t.ref IS NULL AND t.prev IS NULL AND s.aoi_id=$2`, common.StatusFAILED, aoi)
	if err != nil {
		return nil, fmt.Errorf("RootTiles.Scan: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		tile := common.Tile{}
		tile.Scene = common.Scene{AOI: aoi}
		if err := rows.Scan(&tile.ID, &tile.SourceID, &tile.Data, &tile.Scene.ID, &tile.Scene.SourceID, &tile.Scene.Data); err != nil {
			return nil, fmt.Errorf("RootTiles.Scan: %w", err)
		}
		tiles = append(tiles, tile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("RootTiles.Rows.err: %w", err)
	}
	return tiles, nil
}

// LeafTiles implements WorkflowBackend
func (b Backend) LeafTiles(ctx context.Context, aoi string) ([]common.Tile, error) {
	tiles := []common.Tile{}
	rows, err := b.QueryContext(ctx,
		`select t.id, t.source_id, t.data, s.id, s.source_id, s.data
			from tile t join scene s on t.scene_id = s.id
			where t.status != $1 AND NOT EXISTS (SELECT NULL FROM tile t2 WHERE t.id = t2.prev) AND s.aoi_id=$2`, common.StatusFAILED, aoi)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		tile := common.Tile{}
		tile.Scene = common.Scene{AOI: aoi}
		if err := rows.Scan(&tile.ID, &tile.SourceID, &tile.Data, &tile.Scene.ID, &tile.Scene.SourceID, &tile.Scene.Data); err != nil {
			return nil, fmt.Errorf("LeafTiles.Scan: %w", err)
		}
		tiles = append(tiles, tile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("LeafTiles.Rows.err: %w", err)
	}
	return tiles, nil
}

// UpdateTile implements WorkflowBackend
func (b Backend) UpdateTile(ctx context.Context, id int, status common.Status, message *string, resetPrev bool) error {
	var err error

	query := "update tile set status=$1"
	parameters := []interface{}{status, id}
	if message != nil {
		parameters = append(parameters, *message)
		query += ", message=$3"
	}
	if resetPrev {
		query += ", prev=NULL"
	}

	if _, err = b.ExecContext(ctx, query+" where id=$2", parameters...); err != nil {
		return fmt.Errorf("UpdateTile: %w", err)
	}
	return nil
}

// SetTilesStatus implements WorkflowBackend
func (b Backend) SetTilesStatus(ctx context.Context, ids []int, status common.Status) error {
	if _, err := b.ExecContext(ctx, "update tile set status=$1 where id=ANY($2)", status, pq.Array(ids)); err != nil {
		return fmt.Errorf("UpdateTile: %w", err)
	}
	return nil
}

// UpdateNextTilesStatus implements WorkflowBackend
func (b Backend) UpdateNextTilesStatus(ctx context.Context, prevID int, status, sceneStatus, newStatus common.Status) ([]db.Tile, []int, error) {
	rows, err := b.QueryContext(ctx,
		`update tile set status=$1 FROM scene where
			tile.prev=$2 and tile.status=$3 and tile.scene_id=scene.id and scene.status=$4
			RETURNING tile.id, tile.source_id, tile.scene_id, tile.ref, tile.data`,
		newStatus, prevID, status, sceneStatus)
	if err != nil {
		return nil, nil, fmt.Errorf("UpdateNextTilesStatus.QueryContext: %w", err)
	}
	defer rows.Close()

	var tiles []db.Tile
	var scenesID []int
	for rows.Next() {
		tile := db.Tile{}
		scID := 0
		if err := rows.Scan(&tile.ID, &tile.SourceID, &scID, &tile.ReferenceID, &tile.Data); err != nil {
			return nil, nil, fmt.Errorf("UpdateNextTilesStatus.Scan: %w", err)
		}
		tiles = append(tiles, tile)
		scenesID = append(scenesID, scID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("UpdateNextTilesStatus.Rows.err: %w", err)
	}
	return tiles, scenesID, nil
}

// UpdateSceneTilesStatus implements WorkflowBackend
func (b Backend) UpdateSceneTilesStatus(ctx context.Context, sceneID int, status, prevStatus, newStatus common.Status) ([]db.Tile, []db.Tile, []int, error) {
	rows, err := b.QueryContext(ctx,
		`update tile set status=$1 FROM tile prev_tile where tile.scene_id=$2 and tile.status=$3
				and tile.prev=prev_tile.id and prev_tile.status=$4
				RETURNING tile.id, tile.source_id, tile.ref, tile.data, prev_tile.id,prev_tile.source_id,prev_tile.scene_id,prev_tile.data`,
		newStatus, sceneID, status, prevStatus)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("UpdateSceneTilesStatus.QueryContext: %w", err)
	}
	defer rows.Close()

	var ctiles []db.Tile
	var ptiles []db.Tile
	var pscenesID []int
	for rows.Next() {
		ctile := db.Tile{}
		ptile := db.Tile{}
		pscID := 0
		if err := rows.Scan(
			&ctile.ID, &ctile.SourceID, &ctile.ReferenceID, &ctile.Data,
			&ptile.ID, &ptile.SourceID, &pscID, &ptile.Data); err != nil {
			return nil, nil, nil, fmt.Errorf("UpdateSceneTilesStatus.Scan: %w", err)
		}
		ctiles = append(ctiles, ctile)
		ptiles = append(ptiles, ptile)
		pscenesID = append(pscenesID, pscID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("UpdateSceneTilesStatus.Rows.err: %w", err)
	}
	return ctiles, ptiles, pscenesID, nil
}

// UpdateSceneRootTilesStatus implements WorkflowBackend
func (b Backend) UpdateSceneRootTilesStatus(ctx context.Context, sceneID int, status, newStatus common.Status) ([]db.Tile, error) {
	rows, err := b.QueryContext(ctx,
		`update tile set status=$1 where scene_id=$2 and status=$3 and prev IS NULL
		RETURNING tile.id, tile.source_id, tile.ref, tile.data`,
		newStatus, sceneID, status)
	if err != nil {
		return nil, fmt.Errorf("UpdateSceneRootTilesStatus.QueryContext: %w", err)
	}
	defer rows.Close()

	var ctiles []db.Tile
	for rows.Next() {
		ctile := db.Tile{}
		if err := rows.Scan(&ctile.ID, &ctile.SourceID, &ctile.ReferenceID, &ctile.Data); err != nil {
			return nil, fmt.Errorf("UpdateSceneRootTilesStatus.Scan: %w", err)
		}
		ctiles = append(ctiles, ctile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("UpdateSceneRootTilesStatus.Rows.err: %w", err)
	}
	return ctiles, nil
}

// UpdateNextTilesPrevId implements WorkflowBackend
func (b Backend) UpdateNextTilesPrevId(ctx context.Context, oldPrevID int, newPrevID *int) ([]int, error) {
	var rows *sql.Rows
	var err error
	if newPrevID == nil {
		rows, err = b.QueryContext(ctx, "update tile set prev=NULL where prev=$1 returning tile.ID",
			oldPrevID)
	} else {
		rows, err = b.QueryContext(ctx, "update tile set prev=$1 where prev=$2 returning tile.ID",
			*newPrevID, oldPrevID)
	}
	if err != nil {
		return nil, fmt.Errorf("UpdateNextTilesPrevId.QueryContext: %w", err)
	}
	defer rows.Close()

	var bids []int
	for rows.Next() {
		bid := 0
		if err = rows.Scan(&bid); err != nil {
			return nil, fmt.Errorf("UpdateNextTilesPrevId.Scan: %w", err)
		}
		bids = append(bids, bid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("UpdateNextTilesPrevId.Rows.err: %w", err)
	}
	return bids, nil
}

// UpdateRefTiles implements WorkflowBackend
func (b Backend) UpdateRefTiles(ctx context.Context, oldRefID int, newRefID int) error {
	if newRefID == oldRefID {
		return nil
	}

	// Tiles that become roots
	if _, err := b.ExecContext(ctx, "update tile set ref=NULL where id=$1 and ref=$2", newRefID, oldRefID); err != nil {
		return fmt.Errorf("UpdateRefTiles.QueryContext: %w", err)
	}

	// Other tiles
	if _, err := b.ExecContext(ctx, "update tile set ref=$1 where ref=$2", newRefID, oldRefID); err != nil {
		return fmt.Errorf("UpdateRefTiles.QueryContext: %w", err)
	}
	return nil
}
