package entities

import (
	"encoding/json"
	"testing"

	"github.com/airbusgeo/geocube-ingester/common"
)

func TestMarshallScenes(t *testing.T) {
	scenes := Scenes{
		Scenes: []*Scene{
			{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_041D"}, Tags: map[string]string{common.TagIngestionDate: "20190101"}, GeometryWKT: "POLYGON ((8.602359 52.724068,12.720232 53.141727,13.151229 51.523666,9.198775 51.110802,8.602359 52.724068))"},
			{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_06BD"}, Tags: map[string]string{common.TagIngestionDate: "20190102"}, GeometryWKT: "POLYGON ((8.602359 55.724068,12.720232 56.141727,13.151229 54.523666,9.198775 54.110802,8.602359 55.724068))",
				Tiles: []*Tile{{TileLite: TileLite{SourceID: "A44_IW1_4655"}, GeometryWKT: "POLYGON ((9.602359 55.724068,11.720232 56.141727,12.151229 54.523666,10.198775 54.110802,9.602359 55.724068))"}}},
			{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_1242"}, Tags: map[string]string{common.TagIngestionDate: "20190101"}, GeometryWKT: "POLYGON ((8.602359 58.724068,12.720232 59.141727,13.151229 57.523666,9.198775 57.110802,8.602359 58.724068))"},
		},
		Properties: nil,
	}

	geojson, _ := json.Marshal(scenes)
	if string(geojson) != `{"type":"FeatureCollection","features":[{"type":"Feature","id":0,"geometry":{"type":"Polygon","coordinates":[[[8.602359,52.724068],[12.720232,53.141727],[13.151229,51.523666],[9.198775,51.110802],[8.602359,52.724068]]]},"properties":{"aoi":"","data":{"date":"0001-01-01T00:00:00Z","graph_config":null,"graph_name":"","is_retriable":false,"metadata":null,"record_id":"","storage_uri":"","uuid":""},"id":0,"source_id":"S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_041D","tags":{"ingestionDate":"20190101"},"wkt":"POLYGON ((8.602359 52.724068,12.720232 53.141727,13.151229 51.523666,9.198775 51.110802,8.602359 52.724068))"}},{"type":"Feature","id":1,"geometry":{"type":"MultiPolygon","coordinates":[[[[8.602359,55.724068],[12.720232,56.141727],[13.151229,54.523666],[9.198775,54.110802],[8.602359,55.724068]]],[[[9.602359,55.724068],[11.720232,56.141727],[12.151229,54.523666],[10.198775,54.110802],[9.602359,55.724068]]]]},"properties":{"aoi":"","data":{"date":"0001-01-01T00:00:00Z","graph_config":null,"graph_name":"","is_retriable":false,"metadata":null,"record_id":"","storage_uri":"","uuid":""},"id":0,"source_id":"S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_06BD","tags":{"ingestionDate":"20190102"},"tiles":[{"Date":"0001-01-01T00:00:00Z","SceneID":"","SourceID":"A44_IW1_4655","anx_time":0,"data":{"graph_name":"","is_retriable":false,"swath_id":"","tile_nr":0},"id":0,"previous":null,"reference":null,"wkt":"POLYGON ((9.602359 55.724068,11.720232 56.141727,12.151229 54.523666,10.198775 54.110802,9.602359 55.724068))"}],"wkt":"POLYGON ((8.602359 55.724068,12.720232 56.141727,13.151229 54.523666,9.198775 54.110802,8.602359 55.724068))"}},{"type":"Feature","id":2,"geometry":{"type":"Polygon","coordinates":[[[8.602359,58.724068],[12.720232,59.141727],[13.151229,57.523666],[9.198775,57.110802],[8.602359,58.724068]]]},"properties":{"aoi":"","data":{"date":"0001-01-01T00:00:00Z","graph_config":null,"graph_name":"","is_retriable":false,"metadata":null,"record_id":"","storage_uri":"","uuid":""},"id":0,"source_id":"S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_1242","tags":{"ingestionDate":"20190101"},"wkt":"POLYGON ((8.602359 58.724068,12.720232 59.141727,13.151229 57.523666,9.198775 57.110802,8.602359 58.724068))"}}]}` {
		t.Error("wrong geojson got: " + string(geojson))
	}
	newScenes := Scenes{}
	if err := json.Unmarshal(geojson, &newScenes); err != nil {
		t.Error(err)
	}
	if len(newScenes.Scenes) != len(scenes.Scenes) {
		t.Errorf("expecting %d, found %d scenes", len(scenes.Scenes), len(newScenes.Scenes))
	}
	for i, scene := range scenes.Scenes {
		s1, err := json.Marshal(newScenes.Scenes[i])
		if err != nil {
			t.Error(err)
		}
		s2, err := json.Marshal(scene)
		if err != nil {
			t.Error(err)
		}
		if string(s1) != string(s2) {
			t.Errorf("expecting scene %s found %s", scenes.Scenes[i].SourceID, scene.SourceID)
		}
	}
}
