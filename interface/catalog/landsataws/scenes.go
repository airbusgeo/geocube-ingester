package landsataws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/geometry"
)

const (
	LandsatAwsURL         = "https://landsatlook.usgs.gov/stac-server/search"
	LandsatCollectionC2L1 = "landsat-c2l1"
	LandsatCatalogLimit   = 1000
)

type AWSSearchData struct {
	Features       []LandsatFeature `json:"features"`
	Links          []Link           `json:"links"`
	NumberMatched  int              `json:"numberMatched"`
	NumberReturned int              `json:"numberReturned"`
}

type Link struct {
	Body   map[string]interface{} `json:"body"`
	Href   string                 `json:"href"`
	Method string                 `json:"method"`
	Rel    string                 `json:"rel"`
}

type LandsatFeature struct {
	Id          string                  `json:"id"`
	BoundingBox []float64               `json:"bbox"`
	Properties  map[string]interface{}  `json:"properties"`
	Geometry    *geojson.Geometry       `json:"geometry"`
	Assets      map[string]LandsatAsset `json:"assets"`
}

type LandsatAsset struct {
	Title     string                           `json:"title"`
	Alternate map[string]LandsatAssetAlternate `json:"alternate"`
}

type LandsatAssetAlternate struct {
	Href string `json:"href"`
}

type awsSearch struct {
	Bbox        []float64              `json:"bbox,omitempty"`
	Intersects  geojson.Geometry       `json:"intersects,omitempty"`
	Query       map[string]interface{} `json:"query,omitempty"`
	Datetime    string                 `json:"datetime,omitempty"`
	Collections []string               `json:"collections"`
	Ids         []string               `json:"ids,omitempty"`
	Limit       int                    `json:"limit,omitempty"`
	Page        int                    `json:"page,omitempty"`
}

type Provider struct {
	Limit int
}

func (p *Provider) Supports(c common.Constellation) bool {
	switch c {
	case common.Landsat89:
		return true
	}
	return false
}

func (s *Provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	if s.Limit == 0 {
		s.Limit = LandsatCatalogLimit
	}
	geom, err := geometry.GeosToGeom(&aoi)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("SearchScenes(LandsatAws).%w", err)
	}

	startDate := area.StartTime.Format("2006-01-02") + "T00:00:00.000Z"
	endDate := area.EndTime.Format("2006-01-02") + "T23:59:59.999Z"

	// request object is initialized
	req := awsSearch{
		Intersects: geojson.Geometry{Geometry: geom},
		Query:      map[string]interface{}{},
		Datetime:   startDate + "/" + endDate,
	}

	// Only LANDSAT_8 and LANDSAT_9 are currently supported
	switch common.GetConstellationFromString(area.SceneType.Constellation) {
	case common.Landsat89:
		req.Query["platform"] = map[string][]string{"in": {"LANDSAT_8", "LANDSAT_9"}}
		req.Collections = []string{LandsatCollectionC2L1}
	default:
		return entities.Scenes{}, fmt.Errorf("SearchScenes(LandsatAws): constellation not supported: " + area.SceneType.Constellation)
	}

	for k, v := range area.SceneType.Parameters {
		if k == "cloudcoverpercentage" {
			vs := strings.Split(strings.Trim(v[1:], "]"), " TO ")
			if len(vs) != 2 {
				return entities.Scenes{}, fmt.Errorf("SearchScenes(LandsatAws): cloudcoverpercentage must be 'Min TO Max'")
			}
			min, errMin := strconv.Atoi(vs[0])
			max, errMax := strconv.Atoi(vs[1])
			if errMin != nil || errMax != nil {
				return entities.Scenes{}, fmt.Errorf("SearchScenes(LandsatAws): cloudcoverpercentage values must be integers: %s/%s", vs[0], vs[1])
			}
			req.Query["eo:cloud_cover"] = map[string]int{"lte": max, "gte": min}
		}
	}

	// Execute query
	landsatFeatures, err := s.queryLandsatAws(ctx, LandsatAwsURL, req, area.Page, area.Limit)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("SearchScenes(LandsatAws).%w", err)
	}

	// Parse results
	scenes := make([]*entities.Scene, len(landsatFeatures))
	for i, landsatFeature := range landsatFeatures {
		properties := landsatFeature.Properties
		sourceId := landsatFeature.Id
		// Parse date
		date, err := time.Parse(time.RFC3339Nano, properties["datetime"].(string))
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes(LandsatAws).parse datetime property: %w", err)
		}
		productType := fmt.Sprintf("%s_C%s_%s", sourceId[0:2], properties["landsat:collection_number"], properties["landsat:correction"])

		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: sourceId,
				Data: common.SceneAttrs{
					Date:         date,
					TileMappings: map[string]common.TileMapping{},
				},
			},
			Tags: map[string]string{
				common.TagSourceID:                 sourceId,
				common.TagProductType:              productType,
				common.TagCloudCoverPercentage:     fmt.Sprintf("%v", properties["eo:cloud_cover"]),
				common.TagLandCloudCoverPercentage: fmt.Sprintf("%v", properties["landsat:cloud_cover_land"]),
				common.TagIngestionDate:            fmt.Sprintf("%v", properties["datetime"]),
				common.TagSunAzimuth:               fmt.Sprintf("%v", properties["view:sun_azimuth"]),
				common.TagSunElevation:             fmt.Sprintf("%v", properties["view:sun_elevation"]),
			},
			GeometryWKT: wkt.MustEncode(landsatFeature.Geometry.Geometry),
		}

		// Autofill some fields
		scenes[i].AutoFill()
	}

	return entities.Scenes{
		Scenes:     scenes,
		Properties: nil,
	}, nil
}

func (s *Provider) queryLandsatAws(ctx context.Context, url string, searchReq awsSearch, clientPage int, clientLimit int) ([]LandsatFeature, error) {

	pagesToQuery := service.ComputePagesToQuery(clientPage, clientLimit, s.Limit)

	httpMethod := "POST"
	landsatFeatures := []LandsatFeature{}
	for _, pageToQuery := range pagesToQuery {
		//log.Logger(ctx).Sugar().Debugf("queryLandsatAws Search page/limit/firstRowToSelect/lastRowToSelect: %d/%d/%d/%d", pageToQuery.Page, pageToQuery.Limit, pageToQuery.firstRowToSelect, pageToQuery.lastRowToSelect)

		searchReq.Limit = pageToQuery.Limit
		searchReq.Page = pageToQuery.Page + 1

		// request is encoded in json
		reqBody := &bytes.Buffer{}
		err := json.NewEncoder(reqBody).Encode(searchReq)
		if err != nil {
			return nil, fmt.Errorf("queryLandsatAws.json.encode: %w", err)
		}

		req, err := http.NewRequest(httpMethod, url, reqBody)
		if err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/json")

		respBody, err := service.GetBodyRetryReq(req, 4)
		if err != nil {
			return nil, fmt.Errorf("queryLandsatAws.GetBodyRetryReq: %w", err)
		}

		search := &AWSSearchData{}
		err = json.Unmarshal(respBody, search)
		if err != nil {
			return nil, fmt.Errorf("queryLandsatAws.search parse body: (%s)", url)
		}

		// selection of the slice to be applied on returned features
		selectedFeatures := service.QueryGetResult(&pageToQuery, search.Features)
		landsatFeatures = append(landsatFeatures, selectedFeatures...)

		nextFound := false
		for _, link := range search.Links {
			if link.Rel == "next" {
				url = link.Href
				httpMethod = link.Method

				reqBody = &bytes.Buffer{}
				if link.Body != nil {
					if err = json.NewEncoder(reqBody).Encode(link.Body); err != nil {
						return nil, err
					}
				}

				nextFound = true
			}
		}

		if !nextFound {
			break
		}
	}

	return landsatFeatures, nil
}
