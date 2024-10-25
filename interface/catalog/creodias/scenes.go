package creodias

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

type Provider struct {
}

func (s *Provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	// Construct Query
	mapKey := map[string]string{
		//"constellation":         "Collection/Name eq '%%s'",
		//"producttype":           "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '%%s')",
		//"polarisationmode":      "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'polarisationMode' and att/OData.CSC.StringAttribute/Value eq '%%s')",
		//"sensoroperationalmode": "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'sensorOperationalMode' and att/OData.CSC.StringAttribute/Value eq '%%s')",
		//"cloudcoverpercentage":  "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value ge %%s) and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le %%s)",
		//"filename":              "contains(Name,%%s)",
		"producttype":           "productType=%s",
		"polarisationmode":      "polarisation=%s",
		"sensoroperationalmode": "sensorMode=%s",
		"cloudcoverpercentage":  "cloudCover=%s",
	}

	// Construct Query
	parametersMap := map[string]string{}
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		parametersMap["constellation"] = "SENTINEL-1"
		parametersMap[mapKey["producttype"]] = "SLC"
		parametersMap[mapKey["polarisationmode"]] = "VV%26VH"
		parametersMap[mapKey["sensoroperationalmode"]] = "IW"
	case common.Sentinel2:
		parametersMap["constellation"] = "SENTINEL-2"
		parametersMap[mapKey["producttype"]] = "S2MSI1C"
	default:
		return entities.Scenes{}, fmt.Errorf("Creodias: constellation not supported: " + area.SceneType.Constellation)
	}
	for k, v := range area.SceneType.Parameters {
		if nk, ok := mapKey[k]; ok {
			k = nk
		}
		parametersMap[k] = v
	}

	var parameters []string
	var hostUrl string
	for k, v := range parametersMap {
		if k == "filename" {
			log.Logger(ctx).Debug("Creodias: Search by Filename not supported")
			continue
		}
		if k == "constellation" {
			switch entities.GetConstellation(v) {
			case common.Sentinel1:
				hostUrl = "https://datahub.creodias.eu/resto/api/collections/Sentinel1/search.json?"
			case common.Sentinel2:
				hostUrl = "https://datahub.creodias.eu/resto/api/collections/Sentinel2/search.json?"
			}
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
			return entities.Scenes{}, fmt.Errorf("SearchScenes.ConvexHull: %w", err)
		}

		convexhullWKT, err := convexhull.ToWKT()
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes.ToWKT: %w", err)
		}
		//	parameters = append(parameters, fmt.Sprintf("OData.CSC.Intersects(area=geography'SRID=4326;%s')", neturl.QueryEscape(convexhullWKT)))
		parameters = append(parameters, fmt.Sprintf("geometry=%s", neturl.QueryEscape(convexhullWKT)))
	}

	// Append time
	//parameters = append(parameters, fmt.Sprintf("ContentDate/Start gt %v and ContentDate/Start lt%v", area.StartTime, area.EndTime))
	parameters = append(parameters, fmt.Sprintf("startDate=%s&completionDate=%s", area.StartTime.Format("2006-01-02"), area.EndTime.Format("2006-01-02")))

	// Execute query
	rawscenes, err := s.queryCreodias(ctx, hostUrl, strings.Join(parameters, "&"), area.Page, area.Limit)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("SearchScenes.%w", err)
	}

	// Parse results
	scenes := make([]*entities.Scene, len(rawscenes))
	for i, rawscene := range rawscenes {
		// Parse date
		date, err := time.Parse(time.RFC3339Nano, rawscene.Properties.BeginPosition)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("Creodias.searchScenes.TimeParse: %w", err)
		}
		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: rawscene.Properties.Identifier[:len(rawscene.Properties.Identifier)-len(".SAFE")],
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

	return entities.Scenes{Scenes: scenes}, nil
}

type creodiasHits struct {
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

func (s *Provider) queryCreodias(ctx context.Context, baseurl string, query string, page, limit int) ([]creodiasHits, error) {
	// Pagging
	var rawscenes []creodiasHits
	totalPages := "?"

	pageLimit, rows := service.PageLimitRows(page, limit, 1000)

	for nextPage := true; nextPage && (page < pageLimit); page += 1 {
		log.Logger(ctx).Sugar().Debugf("[Creodias] Search page %d/%s", page, totalPages)

		// Load results
		//url := baseurl + query + fmt.Sprintf("&$top=%d&$skip=%d", rows, index)
		url := baseurl + query + fmt.Sprintf("&maxRecords=%d&page=%d", rows, page+1)
		jsonResults, err := service.GetBodyRetry(url, 3)
		if err != nil {
			return nil, fmt.Errorf("queryCreodias.getBodyRetry: %w", err)
		}

		//JSON
		results := struct {
			Status     int `json:"status"`
			Properties struct {
				TotalResults int `json:"totalResults"`
			} `json:"properties"`
			Hits []creodiasHits `json:"features"`
		}{}

		// Read results to retrieve scenes
		if err := json.Unmarshal(jsonResults, &results); err != nil {
			return nil, fmt.Errorf("queryCreodias.Unmarshal : %w (response: %s)", err, jsonResults)
		}

		if results.Status != 0 && results.Status != 200 {
			return nil, fmt.Errorf("queryCreodias : http status %d (response: %s)", results.Status, jsonResults)
		}

		// Merge the results
		rawscenes = append(rawscenes, results.Hits...)

		// Is there a next page ?
		nextPage = page*rows < results.Properties.TotalResults

		if nextPage {
			totalPages = strconv.Itoa(results.Properties.TotalResults/rows + 1)
		}
	}

	return rawscenes, nil
}
