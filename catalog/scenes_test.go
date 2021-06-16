package catalog

import (
	"testing"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
)

func TestRemoveDoubleEntries(t *testing.T) {
	scenes := []*entities.Scene{
		{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_041D"}, Tags: map[string]string{common.TagIngestionDate: "20190101"}},
		{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_06BD"}, Tags: map[string]string{common.TagIngestionDate: "20190102"}},
		{Scene: common.Scene{SourceID: "S1A_IW_SLC__1SDV_20200415T054835_20200415T054902_032134_03B6F4_1242"}, Tags: map[string]string{common.TagIngestionDate: "20190101"}},
	}

	newscenes := removeDoubleEntries(scenes)
	if len(newscenes) != 1 {
		t.Errorf("expecting 1, found %d scenes", len(newscenes))
	}
	if newscenes[0] != scenes[1] {
		t.Errorf("expecting scene %s found %s", scenes[1].SourceID, newscenes[0].SourceID)
	}
}
