package common

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

const (
	ResultTypeTile  = "tile"
	ResultTypeScene = "scene"
)

type TileMapping struct {
	SwathID string `json:"swath_id"`
	TileNr  int    `json:"tile_nr"`
}

type SceneAttrs struct {
	UUID         string                 `json:"uuid"`
	Date         time.Time              `json:"date"`
	TileMappings map[string]TileMapping `json:"tiles"`
	GraphName    string                 `json:"graph_name"`
	GraphConfig  map[string]string      `json:"graph_config"`
	RecordID     string                 `json:"record_id"`
	InstancesID  map[string]string      `json:"instances_id"`
}

type TileAttrs struct {
	SwathID   string `json:"swath_id"`
	TileNr    int    `json:"tile_nr"`
	GraphName string `json:"graph_name"`
}

type Scene struct {
	ID       int        `json:"id"`
	SourceID string     `json:"source_id"`
	AOI      string     `json:"aoi"`
	Data     SceneAttrs `json:"data,omitempty"`
}

type TileToIngest struct {
	PreviousTileID   string    `json:"previous_tile_id"`
	PreviousSceneID  string    `json:"previous_scene_id"`
	ReferenceTileID  string    `json:"reference_tile_id"`
	ReferenceSceneID string    `json:"reference_scene_id"`
	Data             TileAttrs `json:"data"`
}

type SceneToIngest struct {
	Scene
	Tiles map[string]TileToIngest `json:"tiles"`
}

type Tile struct {
	ID       int       `json:"id"`
	SourceID string    `json:"source_id"`
	Scene    Scene     `json:"scene,omitempty"`
	Data     TileAttrs `json:"data,omitempty"`
}

type TileToProcess struct {
	Tile
	Previous  Tile `json:"tile_previous"`
	Reference Tile `json:"tile_reference"`
}

type Result struct {
	Type    string `json:"type"` // scene (ResultTypeScene) or tile (ResultTypeTile)
	ID      int    `json:"id"`
	Status  Status `json:"status"`
	Message string `json:"message"`
}

// Value implements the driver.Value interface
func (a SceneAttrs) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Scan implements the sql.Scanner interface.
func (a *SceneAttrs) Scan(value interface{}) error {
	if value == nil {
		*a = SceneAttrs{}
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &a)
}

// Value implements the driver.Value interface
func (a TileAttrs) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Scan implements the sql.Scanner interface.
func (a *TileAttrs) Scan(value interface{}) error {
	if value == nil {
		*a = TileAttrs{}
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &a)
}
