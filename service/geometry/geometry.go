package geometry

import (
	"fmt"

	"github.com/go-spatial/geom"
	geomwkt "github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

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

// Generates a geom.Geometry from a geos.Geometry
func GeosToGeom(g *geos.Geometry) (geom.Geometry, error) {
	wkt, err := g.ToWKT()
	if err != nil {
		return nil, fmt.Errorf("GeosToGeom.ToWKT: %w", err)
	}
	geometry, err := geomwkt.DecodeString(wkt)
	if err != nil {
		return nil, fmt.Errorf("GeosToGeom.DecodeString: %w", err)
	}

	return geometry, nil
}

var TOLERANCE_GEOG = 0.000001

func WKTUnion(wkts []string, tolerance float64) (string, error) {
	var geoms []*geos.Geometry
	for _, wkt := range wkts {
		geo, err := geos.FromWKT(wkt)
		if err != nil {
			return "", fmt.Errorf("WKTUnion.FromWKT: %w", err)
		}
		geoms = append(geoms, geo)
	}
	aoi, err := Union(geoms, tolerance)
	if err != nil {
		return "", fmt.Errorf("WKTUnion.%w", err)
	}
	wkt, err := aoi.ToWKT()
	if err != nil {
		return "", fmt.Errorf("WKTUnion.ToWKT: %w", err)
	}
	return wkt, nil
}

func Union(geoms []*geos.Geometry, tolerance float64) (*geos.Geometry, error) {
	aoi, err := UnaryUnion(geoms)
	if err == nil {
		if aoi, err = aoi.Simplify(tolerance); err != nil {
			return nil, fmt.Errorf("Union.Simplify: %w", err)
		}
		return aoi, nil
	}
	// Union all failed, retry one by one with simplify
	for _, geom := range geoms {
		if geom, err = geom.Simplify(tolerance); err != nil {
			return nil, fmt.Errorf("Union.Simplify: %w", err)
		}
		if aoi, err = geom.Union(aoi); err != nil {
			return nil, fmt.Errorf("Union: %w", err)
		}
	}
	return aoi, nil
}

func UnaryUnion(geoms []*geos.Geometry) (*geos.Geometry, error) {
	aoi, err := geos.NewCollection(geos.MULTIPOLYGON, geoms...)
	if err != nil {
		return nil, fmt.Errorf("UnaryUnion.NewCollection: %w", err)
	}
	if aoi, err = aoi.UnaryUnion(); err != nil {
		return nil, fmt.Errorf("UnaryUnion.UnaryUnion: %w", err)
	}
	return aoi, nil
}
