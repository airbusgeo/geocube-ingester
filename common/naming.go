package common

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

//go:generate go run github.com/dmarkham/enumer -json -type Constellation

// Constellation defines the kind of satellites
type Constellation int

const (
	Unknown   Constellation = iota
	Sentinel1               // MMM_BB_TTTR_LFPP_YYYYMMDDTHHMMSS_YYYMMDDTHHMMSS_OOOOOO_DDDDDD_CCCC.SAFE
	Sentinel2               // MMM_MSIXXX_YYYYMMDDTHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE or MMM_CCCC_FFFFDDDDDD_ssss_YYYYMMDDTHHMMSS_ROOO_VYYYYMMTDDHHMMSS_YYYYMMTDDHHMMSS.SAFE
	PHR                     // DS_PHR1B_201706161037358_XXX_XX_XXXXXXX_XXXX_XXXXX
	SPOT                    // DS_SPOT7_201806232333174_XXX_XXX_XXX_XXX_XXXXXXX_XXXXX
	Landsat89               // LXSS_LLLL_PPPRRR_YYYYMMDD_yyyymmdd_CX_TX
)

// GetConstellation returns the constellation from the user input
func GetConstellationFromString(input string) Constellation {
	switch strings.ToLower(input) {
	case "sentinel1", "sentinel-1":
		return Sentinel1
	case "sentinel2", "sentinel-2":
		return Sentinel2
	case "landsat89":
		return Landsat89
	case "phr", "pleiades":
		return PHR
	case "spot":
		return SPOT
	}
	return GetConstellationFromProductId(input)
}

func GetConstellationFromProductId(sceneName string) Constellation {
	if strings.HasPrefix(sceneName, "S1") {
		return Sentinel1
	}
	if strings.HasPrefix(sceneName, "S2") {
		return Sentinel2
	}
	if strings.HasPrefix(sceneName, "DS_PHR") {
		return PHR
	}
	if strings.HasPrefix(sceneName, "DS_SPOT") {
		return SPOT
	}
	if regexp.MustCompile("^L[OTC]0[89]").MatchString(sceneName) {
		return Landsat89
	}
	return Unknown
}

func GetDateFromProductId(sceneName string) (time.Time, error) {
	format, err := Info(sceneName)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse("20060102", fmt.Sprintf("%s%s%s", format["YEAR"], format["MONTH"], format["DAY"]))
}

func Info(sceneName string) (map[string]string, error) {
	switch GetConstellationFromProductId(sceneName) {
	case Sentinel1:
		if len(sceneName) < len("MMM_BB_TTTR_LFPP_YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_OOOOOO_DDDDDD_CCCC") {
			return nil, fmt.Errorf("invalid Sentinel1 file name: " + sceneName)
		}
		return map[string]string{
			"SCENE":            sceneName,
			"MISSION_ID":       sceneName[0:3],
			"MISSION_VERSION":  sceneName[2:3],
			"MODE":             sceneName[4:6],
			"PRODUCT_TYPE":     sceneName[7:10],
			"RESOLUTION":       sceneName[10:11],
			"PROCESSING_LEVEL": sceneName[12:13],
			"PRODUCT_CLASS":    sceneName[13:14],
			"POLARISATION":     sceneName[14:16],
			"DATE":             sceneName[17:25],
			"YEAR":             sceneName[17:21],
			"MONTH":            sceneName[21:23],
			"DAY":              sceneName[23:25],
			"TIME":             sceneName[26:32],
			"HOUR":             sceneName[26:28],
			"MINUTE":           sceneName[28:30],
			"SECOND":           sceneName[30:32],
			"ORBIT":            sceneName[49:55],
			"MISSION":          sceneName[56:62],
			"UNIQUE_ID":        sceneName[63:67],
		}, nil
	case Sentinel2:
		if len(sceneName) < len("MMM_MSIXXX_YYYYMMDDTHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Disc.>") {
			return nil, fmt.Errorf("invalid Sentinel2 file name: " + sceneName)
		}
		if sceneName[10] == '_' {
			return map[string]string{
				"SCENE":           sceneName,
				"MISSION_ID":      sceneName[0:3],
				"MISSION_VERSION": sceneName[2:3],
				"PRODUCT_LEVEL":   sceneName[7:10],
				"DATE":            sceneName[11:19],
				"YEAR":            sceneName[11:15],
				"MONTH":           sceneName[15:17],
				"DAY":             sceneName[17:19],
				"TIME":            sceneName[20:26],
				"HOUR":            sceneName[20:22],
				"MINUTE":          sceneName[22:24],
				"SECOND":          sceneName[24:26],
				"PDGS":            sceneName[28:32],
				"ORBIT":           sceneName[34:37],
				"TILE":            sceneName[38:44],
				"LATITUDE_BAND":   sceneName[39:41],
				"GRID_SQUARE":     sceneName[41:42],
				"GRANULE_ID":      sceneName[42:44],
				"PRODUCT_DISC":    sceneName[45:60],
			}, nil
		} else if len(sceneName) < len("MMM_CCCC_FFFFDDDDDD_ssss_YYYYMMDDTHHMMSS_ROOO_VYYYYMMTDDHHMMSS_YYYYMMTDDHHMMSS") {
			return nil, fmt.Errorf("invalid Sentinel2 file name: " + sceneName)
		}
		return map[string]string{
			"SCENE":         sceneName,
			"MISSION_ID":    sceneName[0:3],
			"PRODUCT_LEVEL": sceneName[16:19],
			"ORBIT":         sceneName[42:45],
		}, nil
	case PHR:
		// DS_PHR1A_201006181052297_FR1_PX_E001N43_0612_06488
		if len(sceneName) < len("DS_PHRNN_YYYYMMDDHHMMSSS_RRR_PP_XxxxYyy_KKLL_TTTTT") {
			return nil, fmt.Errorf("invalid Pleiades file name: " + sceneName)
		}
		return map[string]string{
			"MISSION_ID":     sceneName[3:8],
			"DATE":           sceneName[9:23],
			"YEAR":           sceneName[9:13],
			"MONTH":          sceneName[13:15],
			"DAY":            sceneName[15:17],
			"TIME":           sceneName[17:23],
			"HOUR":           sceneName[17:19],
			"MINUTE":         sceneName[19:21],
			"SECOND":         sceneName[21:23],
			"MODE":           sceneName[29:31],
			"LONGITUDE":      sceneName[32:36],
			"LATITUDE":       sceneName[36:39],
			"LONGITUDE_STEP": sceneName[40:42],
			"LATITUDE_STEP":  sceneName[42:44],
		}, nil
	case SPOT:
		// DS_SPOT6_201212051035424_FR1_FR1_FR1_FR1_E002N41_01174
		if len(sceneName) < len("DS_SPOTN_YYYYMMDDHHMMSSS_AAA_aaa_RRR_rrr_XxxxYyy_TTTTT") {
			return nil, fmt.Errorf("invalid Spot file name: " + sceneName)
		}
		return map[string]string{
			"SCENE":      sceneName,
			"MISSION_ID": sceneName[3:8],
			"SAT_NUMBER": sceneName[7:8],
			"DATE":       sceneName[9:23],
			"YEAR":       sceneName[9:13],
			"MONTH":      sceneName[13:15],
			"DAY":        sceneName[15:17],
			"TIME":       sceneName[17:23],
			"HOUR":       sceneName[17:19],
			"MINUTE":     sceneName[19:21],
			"SECOND":     sceneName[21:23],
			"LONGITUDE":  sceneName[41:46],
			"LATITUDE":   sceneName[46:49],
		}, nil
	case Landsat89:
		// LC09_L1GT_166003_20250603_20250603_02_T2
		if len(sceneName) < len("LXSS_LLLL_PPPRRR_YYYYMMDD_yyyymmdd_CX_TX") {
			return nil, fmt.Errorf("invalid Landsat8/9 file name: " + sceneName)
		}
		collectionChar := sceneName[1:2]
		sensorCollection := "oli-tirs"
		if collectionChar == "O" {
			sensorCollection = "oli"
		} else if collectionChar == "T" {
			sensorCollection = "tirs"
		}

		return map[string]string{
			"MISSION_ID": sceneName[0:1] + sceneName[2:4],
			"DATE":       sceneName[17:25],
			"YEAR":       sceneName[17:21],
			"MONTH":      sceneName[21:23],
			"DAY":        sceneName[23:25],
			"COLLECTION": sensorCollection,
			"PATH":       sceneName[10:13],
			"ROW":        sceneName[13:16],
		}, nil
	}
	return nil, fmt.Errorf("Info: constellation not supported")
}

/**
 * FormatBrackets replaces in <str> all {keys} of <info> by the corresponding value
 * keys must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)
 */
func FormatBrackets(str string, infos ...map[string]string) string {
	for _, info := range infos {
		for k, v := range info {
			str = strings.ReplaceAll(str, "{"+k+"}", v)
		}
	}
	return str
}
