package sobloo

import (
	"context"
	"encoding/json"
	"fmt"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/provider"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

type Provider struct {
}

func (s *Provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	// Construct Query
	mapKey := map[string]string{
		"constellation":         "identification.collection:eq",
		"producttype":           "identification.type:eq",
		"polarisationmode":      "acquisition.polarization:eq",
		"sensoroperationalmode": "acquisition.sensorMode:eq",
		"cloudcoverpercentage":  "contentDescription.cloudCoverPercentage:range",
		"filename":              "identification.externalId:like",
	}

	// Construct Query
	parametersMap := map[string]string{}
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		parametersMap[mapKey["constellation"]] = "Sentinel-1"
		parametersMap[mapKey["producttype"]] = "SLC"
		parametersMap[mapKey["polarisationmode"]] = "VV VH"
		parametersMap[mapKey["sensoroperationalmode"]] = "IW"
	case common.Sentinel2:
		parametersMap[mapKey["constellation"]] = "Sentinel-2"
		parametersMap[mapKey["producttype"]] = "S2MSI1C"
	default:
		return entities.Scenes{}, fmt.Errorf("constellation not supported: " + area.SceneType.Constellation)
	}
	for k, v := range area.SceneType.Parameters {
		if nk, ok := mapKey[k]; ok {
			k = nk
		} else {
			k += ":eq"
		}
		parametersMap[k] = v
	}

	var parameters []string
	for k, v := range parametersMap {
		if k == "contentDescription.cloudCoverPercentage:range" {
			v = strings.Replace(v, " TO ", "<", 1)
		}
		parameters = append(parameters, fmt.Sprintf("f=%s:%v", k, v))
	}

	// Append aoi
	{
		convexhull, err := aoi.ConvexHull()
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes.ConvexHull: %w", err)
		}

		convexhullWKT, err := convexhull.ToWKT()
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes.ToWKT: %w", err)
		}
		parameters = append(parameters, "gintersect="+neturl.QueryEscape(convexhullWKT))
	}

	// Append time
	parameters = append(parameters, fmt.Sprintf("f=timeStamp:range:[%d<%d]", area.StartTime.Unix()*1000, area.EndTime.Unix()*1000))

	// Execute query
	rawscenes, err := s.querySobloo(ctx, provider.SoblooHost+provider.SoblooSearch, strings.Join(parameters, "&"))
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("SearchScenes.%w", err)
	}

	// Parse results
	scenes := make([]*entities.Scene, len(rawscenes))
	for i, rawscene := range rawscenes {
		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: rawscene.Data.Identification.Identifier,
				AOI:      area.AOIID,
				Data: common.SceneAttrs{
					UUID:         rawscene.Data.Uuid,
					Date:         time.Unix(rawscene.Data.Acquisition.BeginPosition/1000, 0),
					GraphName:    area.SceneGraphName,
					GraphConfig:  area.GraphConfig,
					TileMappings: map[string]common.TileMapping{},
				},
			},
			Tags: map[string]string{
				common.TagSourceID:       rawscene.Data.Identification.Identifier,
				common.TagUUID:           rawscene.Data.Uuid,
				common.TagIngestionDate:  time.Unix(rawscene.Data.State.IngestionDate/1000, 0).Format(time.RFC3339),
				common.TagOrbitDirection: rawscene.Data.Orbit.OrbitDirection,
				common.TagRelativeOrbit:  fmt.Sprintf("%d", rawscene.Data.Orbit.RelativeOrbitNumber),
				common.TagOrbit:          "undefined",
				common.TagProductType:    rawscene.Data.Identification.ProductType,
			},
			GeometryWKT: wkt.MustEncode(rawscene.MD.Footprint.Geometry),
		}

		scenes[i].AutoFill()

		// Optional tags
		switch entities.GetConstellation(area.SceneType.Constellation) {
		case common.Sentinel1:
			scenes[i].Tags[common.TagPolarisationMode] = rawscene.Data.Acquisition.PolarizationMode
			scenes[i].Tags[common.TagSliceNumber] = "undefined"
			scenes[i].Tags[common.TagLastOrbit] = "undefined"
			scenes[i].Tags[common.TagLastRelativeOrbit] = "undefined"
		case common.Sentinel2:
			scenes[i].Tags[common.TagCloudCoverPercentage] = fmt.Sprintf("%f", rawscene.Data.ContentDescription.CloudCoverPercentage)
		}

		// Copy area tags
		for k, v := range area.RecordTags {
			if _, ok := scenes[i].Tags[k]; !ok {
				scenes[i].Tags[k] = v
			}
		}
	}

	return entities.Scenes{Scenes: scenes}, nil
}

type soblooHits struct {
	Data struct {
		Uuid           string `json:"uid"`
		Identification struct {
			Identifier  string `json:"externalID"`
			ProductType string `json:"type"`
		} `json:"identification"`
		ContentDescription struct {
			CloudCoverPercentage float64 `json:"cloudCoverPercentage"`
		} `json:"contentDescription"`
		Acquisition struct {
			BeginPosition    int64  `json:"beginViewingDate"`
			PolarizationMode string `json:"polarization"`
		} `json:"acquisition"`
		State struct {
			IngestionDate int64 `json:"insertionDate"`
		} `json:"state"`
		Orbit struct {
			OrbitDirection      string `json:"direction"`
			RelativeOrbitNumber int    `json:"relativeNumber"`
		} `json:"orbit"`
	}
	MD struct {
		Footprint geojson.Geometry `json:"geometry"`
	} `json:"md"`
}

func (s *Provider) querySobloo(ctx context.Context, baseurl string, query string) ([]soblooHits, error) {
	// Pagging
	var rawscenes []soblooHits
	totalPages := "?"

	for index, rows, nextPage := 0, 1000, true; nextPage; index += rows {
		log.Logger(ctx).Sugar().Debugf("[SoBloo] Search page %d/%s", index/rows+1, totalPages)

		// Load results
		url := baseurl + query + fmt.Sprintf("&size=%d&from=%d", rows, index)
		jsonResults, err := service.GetBodyRetry(url, 3)
		if err != nil {
			return nil, fmt.Errorf("querySobloo.getBodyRetry: %w", err)
		}

		//JSON
		results := struct {
			Hits    []soblooHits `json:"hits"`
			TotalNb int          `json:"totalnb"`
			Status  int          `json:"status"`
		}{}

		// Read results to retrieve scenes
		if err := json.Unmarshal(jsonResults, &results); err != nil {
			return nil, fmt.Errorf("querySobloo.Unmarshal : %w (response: %s)", err, jsonResults)
		}

		if results.Status != 0 && results.Status != 200 {
			return nil, fmt.Errorf("querySobloo.Unmarshal : %w (response: %s)", err, jsonResults)
		}

		// Merge all elements of the scene into a dict
		rawscenes = append(rawscenes, results.Hits...)

		// Is there a next page ?
		nextPage = index+rows < results.TotalNb

		if nextPage {
			totalPages = strconv.Itoa(results.TotalNb/rows + 1)
		}
	}

	return rawscenes, nil
}
