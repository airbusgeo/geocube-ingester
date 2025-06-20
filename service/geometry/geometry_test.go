package geometry

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/paulsmith/gogeos/geos"
)

func TestGeosToGeom(t *testing.T) {
	polygon, err := geos.FromWKT("POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))")
	if err != nil {
		t.Error(err)
	}
	g, err := GeosToGeom(polygon)
	if err != nil {
		t.Error(err)
	}
	bytes, err := json.Marshal(geojson.Geometry{Geometry: g})
	if err != nil {
		t.Error(err)
	}
	expected := `{"type":"Polygon","coordinates":[[[20,35],[10,30],[10,10],[30,5],[45,20],[20,35]],[[30,20],[20,15],[20,25],[30,20]]]}`
	if string(bytes) != expected {
		t.Errorf("Expect %s found %s", expected, string(bytes))
	}
}

func checkGeomEquality(wkt1, wkt2 string) error {
	geom1, err := geos.FromWKT(wkt1)
	if err != nil {
		return err
	}
	geom2, err := geos.FromWKT(wkt2)
	if err != nil {
		return err
	}
	if equal, err := geom1.Equals(geom2); err != nil {
		return err
	} else if !equal {
		return fmt.Errorf("Not equal")
	}
	return nil
}

func TestGeom(t *testing.T) {
	wktAOI1 := "POLYGON ((129 -11, 130 -11, 130 -12, 129 -12, 129 -11))"
	wktAOI2 := "POLYGON ((130 -12, 130 -11, 131 -11, 131 -12, 130 -12))"
	wktAOI3 := "POLYGON ((129 -11, 131 -11, 131 -12, 129 -12, 129 -11))"

	if wkt, err := WKTUnion([]string{wktAOI1, wktAOI1}, TOLERANCE_GEOG); err != nil {
		t.Error(err.Error())
	} else if err := checkGeomEquality(wkt, wktAOI1); err != nil {
		t.Errorf("expect %s found %s (%v)", wktAOI1, wkt, err)
	}

	if wkt, err := WKTUnion([]string{wktAOI1, wktAOI2}, TOLERANCE_GEOG); err != nil {
		t.Error(err.Error())
	} else if err := checkGeomEquality(wkt, wktAOI3); err != nil {
		t.Errorf("expect %s found %s (%v)", wktAOI3, wkt, err)
	}
}
