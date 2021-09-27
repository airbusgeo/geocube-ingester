package service

import (
	"fmt"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/geojson"
)

// UnmarshalGeometry, merging featureCollections and geometryCollections into a multipolygon
func UnmarshalGeometry(data []byte) (_ geom.Geometry, err error) {
	defer func() {
		// recover from panic if one occured. Set err to nil otherwise.
		if recover() != nil {
			err = fmt.Errorf("UnmarshalGeometry: panic !! Check json structure (e.g. missing 'type' fields)")
		}
	}()

	var g geojson.Geometry
	if err := g.UnmarshalJSON(data); err != nil {
		return g.Geometry, err
	}
	switch geo := g.Geometry.(type) {
	case geojson.FeatureCollection:
		var mp geom.MultiPolygon
		for _, f := range geo.Features {
			if err := mergeMultiPolygons(f.Geometry.Geometry, &mp); err != nil {
				return nil, err
			}
		}
		return mp, nil
	case geojson.Feature:
		return geo.Geometry.Geometry, nil
	default:
		return g.Geometry, nil
	}
}

func mergeMultiPolygons(g geom.Geometry, mp *geom.MultiPolygon) error {
	switch g := g.(type) {
	case geom.MultiPolygon:
		*mp = append(*mp, g.Polygons()...)
	case geom.Polygon:
		*mp = append(*mp, g.LinearRings())
	case geom.Collection:
		for _, g := range g.Geometries() {
			if err := mergeMultiPolygons(g, mp); err != nil {
				return err
			}
		}
	}
	return nil
}
