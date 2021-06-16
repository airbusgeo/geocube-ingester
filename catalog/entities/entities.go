package entities

import (
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/go-spatial/geom/encoding/geojson"
)

// TileLite defined only the needed fields for a Previous or Reference Tile
type TileLite struct {
	SourceID string
	SceneID  string
	Date     time.Time
}

// Tile defined a tile (i.e. burst/granule...) for the catalog
type Tile struct {
	TileLite
	ID          int
	Data        common.TileAttrs
	AnxTime     int
	GeometryWKT string
	Previous    *TileLite
	Reference   *TileLite
	Ingested    bool
}

// Scene is a specialisation of common.Scene for the catalog
type Scene struct {
	common.Scene
	ProductName string // SceneName without the product discriminator (to remove double entries)
	Tags        map[string]string
	GeometryWKT string
	Tiles       []*Tile
	Ingested    bool // This scene has already been ingested
	OwnRecord   bool // The record has been created for this scene
}

type SceneType struct {
	Constellation string
	Parameters    map[string]string
}

// AreaToIngest is the input of the catalog
type AreaToIngest struct {
	AOIID          string            `json:"aoi"`
	AOI            geojson.Geometry  `json:"geometry"`
	StartTime      time.Time         `json:"start_time"`
	EndTime        time.Time         `json:"end_time"`
	SceneType      SceneType         `json:"scene_type"`
	SceneGraphName string            `json:"scene_graph_name"`
	TileGraphName  string            `json:"tile_graph_name"`
	GraphConfig    map[string]string `json:"graph_config"`
	Layers         map[string]struct {
		Variable   string `json:"variable"`
		Instance   string `json:"instance"`
		InstanceID string `json:"instance_id"`
	} `json:"layers"`
	RecordTags map[string]string `json:"record_tags"`
}

const (
	Sentinel1              = "Sentinel1"
	Sentinel2              = "Sentinel2"
	UndefinedConstellation = "undefined"
)

// GetConstellation returns the constellation from the user input
func GetConstellation(constellation string) string {
	constellation = strings.ToLower(constellation)
	switch constellation {
	case "sentinel1", "sentinel-1":
		return Sentinel1
	case "sentinel2", "sentinel-2":
		return Sentinel2
	}
	if strings.HasPrefix(constellation, "s1") {
		return Sentinel1
	}
	if strings.HasPrefix(constellation, "s2") {
		return Sentinel2
	}
	return UndefinedConstellation
}

// AutoFill fills ProductName, Satellite, Constellation
func (s *Scene) AutoFill() {
	var constellation, satellite string
	switch GetConstellation(s.SourceID) {
	case Sentinel1:
		constellation = "SENTINEL1"
		satellite = constellation + s.SourceID[2:3]
		s.ProductName = s.SourceID[0:63]
	case Sentinel2:
		constellation = "SENTINEL2"
		satellite = constellation + s.SourceID[2:3]
		s.ProductName = s.SourceID[0:44]
	default:
		return
	}
	if s.Tags == nil {
		s.Tags = map[string]string{}
	}
	s.Tags[common.TagConstellation] = constellation
	s.Tags[common.TagSatellite] = satellite
}

func (a *AreaToIngest) InstancesID() map[string]string {
	instances := map[string]string{}
	for k, s := range a.Layers {
		instances[k] = s.InstanceID
	}
	return instances
}
