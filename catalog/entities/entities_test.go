package entities

import (
	"encoding/json"
	"testing"

	"github.com/airbusgeo/geocube-ingester/common"
)

func TestMarshallScenes(t *testing.T) {
	scenes := Scenes{
		{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_041D"}, Tags: map[string]string{common.TagIngestionDate: "20190101"}, GeometryWKT: "POLYGON ((8.602359 52.724068,12.720232 53.141727,13.151229 51.523666,9.198775 51.110802,8.602359 52.724068))"},
		{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_06BD"}, Tags: map[string]string{common.TagIngestionDate: "20190102"}, GeometryWKT: "POLYGON ((8.602359 55.724068,12.720232 56.141727,13.151229 54.523666,9.198775 54.110802,8.602359 55.724068))",
			Tiles: []*Tile{{TileLite: TileLite{SourceID: "A44_IW1_4655"}, GeometryWKT: "POLYGON ((9.602359 55.724068,11.720232 56.141727,12.151229 54.523666,10.198775 54.110802,9.602359 55.724068))"}}},
		{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_1242"}, Tags: map[string]string{common.TagIngestionDate: "20190101"}, GeometryWKT: "POLYGON ((8.602359 58.724068,12.720232 59.141727,13.151229 57.523666,9.198775 57.110802,8.602359 58.724068))"},
	}

	geojson, _ := json.Marshal(scenes)
	if string(geojson) != `{"type":"FeatureCollection","features":[{"type":"Feature","id":0,"geometry":{"type":"Polygon","coordinates":[[[8.602359,52.724068],[12.720232,53.141727],[13.151229,51.523666],[9.198775,51.110802],[8.602359,52.724068]]]},"properties":{"GeometryWKT":"POLYGON ((8.602359 52.724068,12.720232 53.141727,13.151229 51.523666,9.198775 51.110802,8.602359 52.724068))","Ingested":false,"OwnRecord":false,"ProductName":"","Tags":{"ingestionDate":"20190101"},"Tiles":null,"aoi":"","data":{"date":"0001-01-01T00:00:00Z","graph_config":null,"graph_name":"","instances_id":null,"record_id":"","tiles":null,"uuid":""},"id":0,"source_id":"S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_041D"}},{"type":"Feature","id":1,"geometry":{"type":"MultiPolygon","coordinates":[[[[8.602359,55.724068],[12.720232,56.141727],[13.151229,54.523666],[9.198775,54.110802],[8.602359,55.724068]]],[[[9.602359,55.724068],[11.720232,56.141727],[12.151229,54.523666],[10.198775,54.110802],[9.602359,55.724068]]]]},"properties":{"GeometryWKT":"POLYGON ((8.602359 55.724068,12.720232 56.141727,13.151229 54.523666,9.198775 54.110802,8.602359 55.724068))","Ingested":false,"OwnRecord":false,"ProductName":"","Tags":{"ingestionDate":"20190102"},"Tiles":[{"AnxTime":0,"Data":{"graph_name":"","swath_id":"","tile_nr":0},"Date":"0001-01-01T00:00:00Z","GeometryWKT":"POLYGON ((9.602359 55.724068,11.720232 56.141727,12.151229 54.523666,10.198775 54.110802,9.602359 55.724068))","ID":0,"Ingested":false,"Previous":null,"Reference":null,"SceneID":"","SourceID":"A44_IW1_4655"}],"aoi":"","data":{"date":"0001-01-01T00:00:00Z","graph_config":null,"graph_name":"","instances_id":null,"record_id":"","tiles":null,"uuid":""},"id":0,"source_id":"S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_06BD"}},{"type":"Feature","id":2,"geometry":{"type":"Polygon","coordinates":[[[8.602359,58.724068],[12.720232,59.141727],[13.151229,57.523666],[9.198775,57.110802],[8.602359,58.724068]]]},"properties":{"GeometryWKT":"POLYGON ((8.602359 58.724068,12.720232 59.141727,13.151229 57.523666,9.198775 57.110802,8.602359 58.724068))","Ingested":false,"OwnRecord":false,"ProductName":"","Tags":{"ingestionDate":"20190101"},"Tiles":null,"aoi":"","data":{"date":"0001-01-01T00:00:00Z","graph_config":null,"graph_name":"","instances_id":null,"record_id":"","tiles":null,"uuid":""},"id":0,"source_id":"S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_1242"}}]}` {
		t.Error("wrong geojson got: " + string(geojson))
	}
	newScenes := Scenes{}
	if err := json.Unmarshal(geojson, &newScenes); err != nil {
		t.Error(err)
	}
	if len(newScenes) != len(scenes) {
		t.Errorf("expecting %d, found %d scenes", len(scenes), len(newScenes))
	}
	for i, scene := range scenes {
		s1, err := json.Marshal(newScenes[i])
		if err != nil {
			t.Error(err)
		}
		s2, err := json.Marshal(scene)
		if err != nil {
			t.Error(err)
		}
		if string(s1) != string(s2) {
			t.Errorf("expecting scene %s found %s", scenes[i].SourceID, scene.SourceID)
		}
	}
}
