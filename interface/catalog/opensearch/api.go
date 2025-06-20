package opensearch

// Opensearch specificiations https://github.com/dewitt/opensearch/blob/master/opensearch-1-1-draft-6.md

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
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

type Hits struct {
	Uuid       string           `json:"id"`
	Footprint  geojson.Geometry `json:"geometry"`
	Properties struct {
		Identifier           string  `json:"title"`
		BeginPosition        string  `json:"startDate"`
		IngestionDate        string  `json:"published"`
		ProductType          string  `json:"productType"`
		CloudCoverPercentage float64 `json:"cloudCover"`
		OrbitDirection       string  `json:"orbitDirection"`
		RelativeOrbitNumber  int     `json:"relativeOrbitNumber"`
		OrbitNumber          int     `json:"orbitNumber"`
		Polarisation         string  `json:"polarisation"`
	} `json:"properties"`
}

type Config struct {
	Provider string
	BaseUrl  string
}

func ConstructQuery(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry, hostUrl string) (string, error) {
	// Construct Query
	mapKey := map[string]string{
		"producttype":           "productType=%s",
		"polarisationmode":      "polarisation=%s",
		"sensoroperationalmode": "sensorMode=%s",
		"cloudcoverpercentage":  "cloudCover=%s",
	}

	// Construct Query
	parametersMap := map[string]string{}
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		parametersMap[mapKey["producttype"]] = "SLC"
		parametersMap[mapKey["polarisationmode"]] = "VV%26VH"
		parametersMap[mapKey["sensoroperationalmode"]] = "IW"
	case common.Sentinel2:
		parametersMap[mapKey["producttype"]] = "S2MSI1C"
	default:
		return "", fmt.Errorf("OpenSearch: constellation not supported: " + area.SceneType.Constellation)
	}
	for k, v := range area.SceneType.Parameters {
		if nk, ok := mapKey[k]; ok {
			k = nk
		}
		parametersMap[k] = v
	}

	var parameters []string
	for k, v := range parametersMap {
		if k == "filename" {
			log.Logger(ctx).Debug("OpenSearch: Search by Filename not supported")
			continue
		}
		if strings.Contains(k, "polarisation") {
			v = strings.Replace(v, " ", "%26", 1)
		} else if strings.Contains(k, "cloudCover") {
			v = strings.Replace(v, " TO ", ",", 1)
		}
		parameters = append(parameters, fmt.Sprintf(k, v))
	}

	// Append aoi
	{
		convexhull, err := aoi.ConvexHull()
		if err != nil {
			return "", fmt.Errorf("OpenSearch.ConvexHull: %w", err)
		}

		convexhullWKT, err := convexhull.ToWKT()
		if err != nil {
			return "", fmt.Errorf("OpenSearch.ToWKT: %w", err)
		}
		//	parameters = append(parameters, fmt.Sprintf("OData.CSC.Intersects(area=geography'SRID=4326;%s')", neturl.QueryEscape(convexhullWKT)))
		parameters = append(parameters, fmt.Sprintf("geometry=%s", neturl.QueryEscape(convexhullWKT)))
	}

	// Append time
	//parameters = append(parameters, fmt.Sprintf("ContentDate/Start gt %v and ContentDate/Start lt%v", area.StartTime, area.EndTime))
	parameters = append(parameters, fmt.Sprintf("startDate=%s&completionDate=%s", area.StartTime.Format("2006-01-02T15:04:05.999Z"), area.EndTime.Format("2006-01-02T15:04:05.999Z")))

	return strings.Join(parameters, "&"), nil
}

func Query(ctx context.Context, query string, config Config, page, limit, catalogLimit int) ([]Hits, error) {
	var rawscenes []Hits
	totalPages := "?"

	for _, queryParams := range service.ComputePagesToQuery(page, limit, catalogLimit) {
		log.Logger(ctx).Sugar().Debugf("[%s] Search page %d/%s", config.Provider, queryParams.Page, totalPages)

		// Load results
		url := config.BaseUrl + query + fmt.Sprintf("&maxRecords=%d&page=%d", queryParams.Limit, queryParams.Page+1)
		jsonResults, err := service.GetBodyRetry(url, 3)
		if err != nil {
			return nil, fmt.Errorf("query.getBodyRetry: %w", err)
		}

		//JSON
		results := struct {
			Status     int `json:"status"`
			Properties struct {
				TotalResults int `json:"totalResults"`
				Links        []struct {
					Rel  string `json:"rel"`
					Href string `json:"href"`
				}
			} `json:"properties"`
			Hits []Hits `json:"features"`
		}{}

		// Read results to retrieve scenes
		if err := json.Unmarshal(jsonResults, &results); err != nil {
			return nil, fmt.Errorf("query.Unmarshal : %w (response: %s)", err, jsonResults)
		}

		if results.Status != 0 && results.Status != 200 {
			return nil, fmt.Errorf("query : http status %d (response: %s)", results.Status, jsonResults)
		}

		// Merge the results
		rawscenes = append(rawscenes, service.QueryGetResult(&queryParams, results.Hits)...)

		// Is there a next page ?
		nextPage := false
		for _, link := range results.Properties.Links {
			if strings.ToLower(link.Rel) == "next" && link.Href != "" {
				nextPage = true
			}
		}

		if !nextPage || len(rawscenes) == limit {
			break
		}
		totalPages = strconv.Itoa(results.Properties.TotalResults/queryParams.Limit + 1)
	}

	return rawscenes, nil
}

func Parse(area *entities.AreaToIngest, hits []Hits) (entities.Scenes, error) {
	// Parse results
	scenes := make([]*entities.Scene, len(hits))
	for i, rawscene := range hits {
		// Parse date
		date, err := time.Parse(time.RFC3339Nano, rawscene.Properties.BeginPosition)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("Copernicus.searchScenes.TimeParse: %w", err)
		}

		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: strings.TrimSuffix(rawscene.Properties.Identifier, ".SAFE"),
				Data: common.SceneAttrs{
					UUID:         rawscene.Uuid,
					Date:         date,
					TileMappings: map[string]common.TileMapping{},
				},
			},
			Tags: map[string]string{
				common.TagSourceID:       rawscene.Properties.Identifier,
				common.TagUUID:           rawscene.Uuid,
				common.TagIngestionDate:  rawscene.Properties.IngestionDate,
				common.TagOrbitDirection: rawscene.Properties.OrbitDirection,
				common.TagRelativeOrbit:  fmt.Sprintf("%d", rawscene.Properties.RelativeOrbitNumber),
				common.TagOrbit:          fmt.Sprintf("%d", rawscene.Properties.OrbitNumber),
				common.TagProductType:    rawscene.Properties.ProductType,
			},
			GeometryWKT: wkt.MustEncode(rawscene.Footprint.Geometry),
		}

		// Autofill some fields
		scenes[i].AutoFill()

		// Optional tags
		switch entities.GetConstellation(area.SceneType.Constellation) {
		case common.Sentinel1:
			scenes[i].Tags[common.TagPolarisationMode] = rawscene.Properties.Polarisation
			scenes[i].Tags[common.TagSliceNumber] = "undefined"
		case common.Sentinel2:
			scenes[i].Tags[common.TagCloudCoverPercentage] = fmt.Sprintf("%f", rawscene.Properties.CloudCoverPercentage)
		}
	}

	return entities.Scenes{
		Scenes:     scenes,
		Properties: nil,
	}, nil
}
