package common

import (
	"testing"
)

func checkKeyValue(t *testing.T, format map[string]string, key, value string) {
	if v, ok := format[key]; !ok {
		t.Errorf("key %s not found", key)
	} else if v != value {
		t.Errorf("expected %s for key %s, got %s", value, key, v)
	}
}

func TestInfo(t *testing.T) {
	if _, err := Info("S2B_MSIL1C_20190108T104429_N0207_R008_T32UNF_20190108T12485"); err == nil {
		t.Errorf("too short file name")
	}
	if format, err := Info("S2B_MSIL1C_20190108T104429_N0207_R008_T32UNF_20190108T124859.SAFE"); err != nil {
		t.Errorf(err.Error())
	} else {
		checkKeyValue(t, format, "MISSION_ID", "S2B")
		checkKeyValue(t, format, "PRODUCT_LEVEL", "L1C")
		checkKeyValue(t, format, "DATE", "20190108")
		checkKeyValue(t, format, "YEAR", "2019")
		checkKeyValue(t, format, "MONTH", "01")
		checkKeyValue(t, format, "DAY", "08")
		checkKeyValue(t, format, "TIME", "104429")
		checkKeyValue(t, format, "HOUR", "10")
		checkKeyValue(t, format, "MINUTE", "44")
		checkKeyValue(t, format, "SECOND", "29")
		checkKeyValue(t, format, "PDGS", "0207")
		checkKeyValue(t, format, "ORBIT", "008")
		checkKeyValue(t, format, "TILE", "T32UNF")
		checkKeyValue(t, format, "LATITUDE_BAND", "32")
		checkKeyValue(t, format, "GRID_SQUARE", "U")
		checkKeyValue(t, format, "GRANULE_ID", "NF")
	}
	if _, err := Info("S1A_IW_SLC__1SDV_20190115T170106_20190115T170133_025491_02D361_7F7"); err == nil {
		t.Errorf("too short file name")
	}
	if format, err := Info("S1A_IW_SLC__1SDV_20190115T170106_20190115T170133_025491_02D361_7F7C"); err != nil {
		t.Errorf(err.Error())
	} else {
		checkKeyValue(t, format, "MISSION_ID", "S1A")
		checkKeyValue(t, format, "MODE", "IW")
		checkKeyValue(t, format, "PRODUCT_TYPE", "SLC")
		checkKeyValue(t, format, "RESOLUTION", "_")
		checkKeyValue(t, format, "PROCESSING_LEVEL", "1")
		checkKeyValue(t, format, "PRODUCT_CLASS", "S")
		checkKeyValue(t, format, "POLARISATION", "DV")
		checkKeyValue(t, format, "DATE", "20190115")
		checkKeyValue(t, format, "YEAR", "2019")
		checkKeyValue(t, format, "MONTH", "01")
		checkKeyValue(t, format, "DAY", "15")
		checkKeyValue(t, format, "TIME", "170106")
		checkKeyValue(t, format, "HOUR", "17")
		checkKeyValue(t, format, "MINUTE", "01")
		checkKeyValue(t, format, "SECOND", "06")
		checkKeyValue(t, format, "ORBIT", "025491")
		checkKeyValue(t, format, "MISSION", "02D361")
		checkKeyValue(t, format, "UNIQUE_ID", "7F7C")
	}
}
