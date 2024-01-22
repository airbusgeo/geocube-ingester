package entities

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/go-spatial/geom/encoding/wkt"
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
	Constellation string `json:"constellation"`
	Parameters    map[string]string
}

// AreaToIngest is the input of the catalog
type AreaToIngest struct {
	AOIID          string `json:"name"`
	AOI            geom.Geometry
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
	RecordTags      map[string]string `json:"record_tags"`
	AnnotationsURLs []string          `json:"annotations_urls"`
	IsRetriable     bool              `json:"is_retriable"`
	RetryCount      int               `json:"retry_count"`
}

// GetConstellation returns the constellation from the user input
func GetConstellation(constellation string) common.Constellation {
	switch strings.ToLower(constellation) {
	case "sentinel1", "sentinel-1":
		return common.Sentinel1
	case "sentinel2", "sentinel-2":
		return common.Sentinel2
	case "phr":
		return common.PHR
	case "spot":
		return common.SPOT
	}
	return common.GetConstellationFromProductId(constellation)
}

// AutoFill fills ProductName, Satellite, Constellation
func (s *Scene) AutoFill() {
	var constellation, satellite string
	switch GetConstellation(s.SourceID) {
	case common.Sentinel1:
		constellation = "SENTINEL1"
		satellite = constellation + s.SourceID[2:3]
		s.ProductName = s.SourceID[0:63]
	case common.Sentinel2:
		constellation = "SENTINEL2"
		satellite = constellation + s.SourceID[2:3]
		s.ProductName = s.SourceID[0:26] + "_NXXYY" + s.SourceID[32:]
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

// UnmarshalJSON implements the json.Unmarshaler interface for AreaToIngest
func (area *AreaToIngest) UnmarshalJSON(data []byte) error {
	var err error
	if area.AOI, err = service.UnmarshalGeometry(data); err != nil {
		return err
	}

	type AreaToIngesterUnmarshaller AreaToIngest
	if err := json.Unmarshal(data, (*AreaToIngesterUnmarshaller)(area)); err != nil {
		return err
	}

	return nil
}

func (scene *Scene) toFeature() (geojson.Feature, error) {
	var err error
	feature := geojson.Feature{}

	feature.Geometry.Geometry, err = wkt.DecodeString(scene.GeometryWKT)
	if err != nil {
		return feature, fmt.Errorf("ToFeature.DecodeString: %w", err)
	}
	if p, ok := feature.Geometry.Geometry.(geom.MultiPolygon); ok && len(p.Polygons()) == 1 {
		feature.Geometry.Geometry = geom.Polygon(p.Polygons()[0])
	}
	if p, ok := feature.Geometry.Geometry.(geom.Polygon); len(scene.Tiles) != 0 && ok {
		multipolygon := geom.MultiPolygon{p.LinearRings()}
		for _, tile := range scene.Tiles {
			tileGeom, err := wkt.DecodeString(tile.GeometryWKT)
			if err != nil {
				return feature, fmt.Errorf("ToFeature.Tile.DecodeString: %w", err)
			}
			if p, ok := tileGeom.(geom.Polygon); ok {
				multipolygon = append(multipolygon, p)
			}
		}
		feature.Geometry.Geometry = multipolygon
	}

	b, err := json.Marshal(scene)
	if err != nil {
		return feature, fmt.Errorf("ToFeature.Marshal: %w", err)
	}
	if err := json.Unmarshal(b, &feature.Properties); err != nil {
		return feature, fmt.Errorf("ToFeature.Unmarshal: %w", err)
	}
	return feature, nil
}

func (scene *Scene) fromFeature(feature geojson.Feature) error {
	b, err := json.Marshal(feature.Properties)
	if err != nil {
		return fmt.Errorf("FromFeature.Marshal: %w", err)
	}
	if err := json.Unmarshal(b, scene); err != nil {
		return fmt.Errorf("FromFeature.Unmarshal: %w", err)
	}
	return nil
}

type Scenes struct {
	Scenes     []*Scene
	Properties map[string]string
}

// UnmarshalJSON implements the json.Unmarshaler interface for Scenes
func (scenes *Scenes) UnmarshalJSON(data []byte) error {
	// Load FeatureCollection
	fc := geojson.FeatureCollection{}
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("UnmarshalJSON: %w", err)
	}
	// Convert FeatureCollection to a list of scenes
	scenes.Scenes = make([]*Scene, len(fc.Features))
	for i, feature := range fc.Features {
		scenes.Scenes[i] = &Scene{}
		if err := scenes.Scenes[i].fromFeature(feature); err != nil {
			return fmt.Errorf("UnmarshalJSON.%w", err)
		}
	}
	return nil
}

// MarshalJSON implements the json.Unmarshaler interface for Scenes
func (scenes Scenes) MarshalJSON() ([]byte, error) {
	var err error

	// Create FeatureCollection to hold the scene
	fc := featureCollection{
		FeatureCollection: geojson.FeatureCollection{
			Features: make([]geojson.Feature, len(scenes.Scenes)),
		},
		Properties: scenes.Properties,
	}
	for i, scene := range scenes.Scenes {
		if fc.Features[i], err = scene.toFeature(); err != nil {
			return nil, fmt.Errorf("MarshalJSON.%w", err)
		}
		id := uint64(i)
		fc.Features[i].ID = &id
	}
	// Marshal FeatureCollection
	return json.Marshal(fc)
}

type featureCollection struct {
	geojson.FeatureCollection
	Properties map[string]string `json:"properties,omitempty"`
}
