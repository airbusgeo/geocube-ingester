package oneatlas

import (
	"context"
	"testing"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	geomwkt "github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

func query(t *testing.T, p *provider, area *entities.AreaToIngest, aoi geos.Geometry, page, limit, expected int, next bool) {
	/*ctx := context.Background()
	area.Page = page
	area.Limit = limit
	p.limit = 5
	scenes, err := p.SearchScenes(ctx, area, aoi)
	if err != nil {
		t.Errorf("%v", err)
	}
	if len(scenes.Scenes) != expected {
		t.Errorf("Expecting %d hits got %d", expected, len(scenes.Scenes))
	}
	if (next && scenes.Properties["next"] == "false") || (!next && scenes.Properties["next"] == "true") {
		t.Errorf("Expecting next=%v hits got %s", next, scenes.Properties["next"])
	}*/
}

func TestQueryOneAtlas(t *testing.T) {
	wkt := "MULTIPOLYGON (((1 44, 1 43, 2 43, 2 44, 1 44)))"
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
		StartTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
		SceneType: entities.SceneType{
			Constellation: "phr",
			Parameters: map[string]string{
				"productType": "bundle",
				"cloudCover":  "[0, 80]",
			},
		},
	}

	p, cncl := NewOneAtlasProvider(context.Background(), "APIKEY", "", OneAtlasSearchEndpoint, OneAtlasOrderEndpoint, OneAtlasAuthenticationEndpoint)
	defer cncl()
	query(t, p, &area, *geom, 0, 10, 9, false)
	query(t, p, &area, *geom, 0, 3, 3, true)
	query(t, p, &area, *geom, 1, 9, 0, false)
	query(t, p, &area, *geom, 1, 5, 4, false)
	query(t, p, &area, *geom, 1, 2, 2, true)
}
