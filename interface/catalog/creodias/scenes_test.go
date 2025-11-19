package creodias

import (
	"context"
	"testing"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	geomwkt "github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

func query(t *testing.T, area *entities.AreaToIngest, aoi geos.Geometry, page, limit, expected int, next bool) {
	ctx := context.Background()
	area.Page = page
	area.Limit = limit
	p := Provider{Limit: 5}
	scenes, err := p.SearchScenes(ctx, area, aoi)
	if err != nil {
		t.Errorf("%v", err)
	}
	if len(scenes.Scenes) != expected {
		t.Errorf("Expecting %d hits got %d", expected, len(scenes.Scenes))
	}
	if (next && scenes.Properties["next"] == "false") || (!next && scenes.Properties["next"] == "true") {
		t.Errorf("Expecting next=%v hits got %s", next, scenes.Properties["next"])
	}
}

func TestQueryCreodias(t *testing.T) {
	wkt := "MULTIPOLYGON (((10.06123047 54.88637695, 9.957128906 54.87246094, 9.90390625 54.89663086, 9.80625 54.90600586, 9.771191406 55.05991211, 9.78125 55.06904297, 9.830371094 55.05825195, 9.998828125 54.98647461, 10.05771484 54.90791016, 10.06123047 54.88637695)))"
	geometry, err := geomwkt.DecodeString(wkt)
	if err != nil {
		t.Fatalf("%v", err)
	}
	geom, err := geos.FromWKT(wkt)
	if err != nil {
		t.Fatalf("%v", err)
	}
	area := entities.AreaToIngest{
		AOI:       geometry,
		StartTime: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2022, 1, 17, 23, 0, 0, 0, time.UTC),
		SceneType: entities.SceneType{
			Constellation: "sentinel2",
			Parameters: map[string]string{
				"cloudcoverpercentage": "[0 TO 80]",
			},
		},
	}

	query(t, &area, *geom, 0, 8, 7, false)
	query(t, &area, *geom, 0, 3, 3, false)
	query(t, &area, *geom, 1, 7, 0, false)
	query(t, &area, *geom, 1, 4, 3, false)
	query(t, &area, *geom, 1, 2, 2, true)
}
