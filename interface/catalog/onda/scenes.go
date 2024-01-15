package onda

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

type Provider struct {
}

func (s *Provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	// Construct Query
	mapKey := map[string]string{
		"constellation":         "platformName:%s",
		"filename":              "name:%s*",
		"producttype":           "productType:*%s*",
		"polarisationmode":      "polarisationChannels:%s",
		"sensoroperationalmode": "sensorOperationalMode:%s",
		"cloudcoverpercentage":  "cloudCoverPercentage:%s",
	}

	// Construct Query
	parametersMap := map[string]string{}
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		parametersMap[mapKey["constellation"]] = "Sentinel-1"
		parametersMap[mapKey["producttype"]] = "SLC"
		parametersMap[mapKey["polarisationmode"]] = "VV,VH"
		parametersMap[mapKey["sensoroperationalmode"]] = "IW"
	case common.Sentinel2:
		parametersMap[mapKey["constellation"]] = "Sentinel-2"
		parametersMap[mapKey["producttype"]] = "S2MSI1C"
	default:
		return entities.Scenes{}, fmt.Errorf("Onda: constellation not supported: " + area.SceneType.Constellation)
	}
	for k, v := range area.SceneType.Parameters {
		if nk, ok := mapKey[k]; ok {
			k = nk
		}
		if strings.Contains(k, "polarisation") {
			v = strings.Replace(v, " ", ",", 1)
		}
		parametersMap[k] = v
	}

	var parameters []string
	for k, v := range parametersMap {
		parameters = append(parameters, fmt.Sprintf(k, v))
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
		parameters = append(parameters, fmt.Sprintf("footprint:%%22Intersects(%s)%%22", convexhullWKT))
	}

	// Append time
	parameters = append(parameters, fmt.Sprintf("beginPosition:[%s TO %s]", area.StartTime.Format("2006-01-02T15:04:05.000Z"), area.EndTime.Format("2006-01-02T15:04:05.000Z")))

	// Execute query
	options := "$format=json&$expand=Metadata&$select=id,name,beginPosition,creationDate,footprint"
	search := strings.ReplaceAll(strings.Join(parameters, " AND "), " ", "%20")
	rawscenes, err := s.queryOnda(ctx, "https://catalogue.onda-dias.eu/dias-catalogue/Products?"+options+"&$search=%22("+search+")%22") //+neturl.QueryEscape(strings.Join(parameters, " AND "))+"%22")
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("SearchScenes.%w", err)
	}

	// Parse results
	scenes := make([]*entities.Scene, len(rawscenes))
	for i, rawscene := range rawscenes {
		// Parse date
		date, err := time.Parse(time.RFC3339Nano, rawscene.BeginPosition)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("Onda.searchScenes.TimeParse: %w", err)
		}

		// Parse aoi
		wktAOI := strings.ToUpper(rawscene.Footprint)
		if _, err := wkt.DecodeString(wktAOI); err != nil {
			return entities.Scenes{}, fmt.Errorf("Onda.searchScenes.wktDecodeString[%s]: %w", wktAOI, err)
		}

		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: rawscene.Name[:len(rawscene.Name)-len(".zip")],
				Data: common.SceneAttrs{
					UUID:         rawscene.Uuid,
					Date:         date,
					TileMappings: map[string]common.TileMapping{},
				},
			},
			Tags: map[string]string{
				common.TagSourceID:       rawscene.Name,
				common.TagUUID:           rawscene.Uuid,
				common.TagIngestionDate:  rawscene.IngestionDate,
				common.TagOrbitDirection: rawscene.Properties["orbitDirection"],
				common.TagRelativeOrbit:  rawscene.Properties["relativeOrbitNumber"],
				common.TagOrbit:          rawscene.Properties["orbitNumber"],
				common.TagProductType:    rawscene.Properties["productType"],
			},
			GeometryWKT: rawscene.Footprint,
		}

		scenes[i].AutoFill()

		// Optional tags
		switch entities.GetConstellation(area.SceneType.Constellation) {
		case common.Sentinel1:
			scenes[i].Tags[common.TagPolarisationMode] = rawscene.Properties["polarisation"]
			scenes[i].Tags[common.TagSliceNumber] = "undefined"
		case common.Sentinel2:
			scenes[i].Tags[common.TagCloudCoverPercentage] = rawscene.Properties["cloudCoverPercentage"]
		}
	}

	return entities.Scenes{Scenes: scenes}, nil
}

type ondaHits struct {
	Uuid          string `json:"id"`
	Name          string `json:"name"`
	Footprint     string `json:"footprint"`
	BeginPosition string `json:"beginPosition"`
	IngestionDate string `json:"creationDate"`
	Metadata      []struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	} `json:"Metadata"`
	Properties map[string]string
}

func (s *Provider) queryOnda(ctx context.Context, query string) ([]ondaHits, error) {
	// Pagging
	var rawscenes []ondaHits
	nextPage := true
	totalPages := "?"

	for index, rows := 0, 1000; nextPage; index += rows {
		log.Logger(ctx).Sugar().Debugf("[Onda] Search page %d/%s", index/rows+1, totalPages)
		// Load results
		url := query + fmt.Sprintf("&$top=%d", rows)
		if index != 0 {
			url += fmt.Sprintf("&$skip=%d", index)
		}
		jsonResults, err := service.GetBodyRetry(url, 3)
		if err != nil {
			return nil, fmt.Errorf("queryOnda.getBodyRetry: %w", err)
		}

		//JSON
		results := struct {
			Status int        `json:"status"`
			Hits   []ondaHits `json:"value"`
		}{}

		// Read results to retrieve scenes
		if err := json.Unmarshal(jsonResults, &results); err != nil {
			return nil, fmt.Errorf("queryOnda.Unmarshal : %w (response: %s)", err, jsonResults)
		}

		if results.Status != 0 && results.Status != 200 {
			return nil, fmt.Errorf("queryOnda : http status %d (response: %s)", results.Status, jsonResults)
		}

		// Transform metadata into a dict
		for _, h := range results.Hits {
			h.Properties = map[string]string{}
			for _, m := range h.Metadata {
				h.Properties[m.ID] = m.Value
			}
		}

		// Merge the results
		rawscenes = append(rawscenes, results.Hits...)

		// Is there a next page ?
		nextPage = len(results.Hits) > 0
	}

	return rawscenes, nil
}
