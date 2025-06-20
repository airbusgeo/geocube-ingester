package copernicus

import (
	"context"
	"encoding/json"
	"fmt"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/opensearch"
	"github.com/airbusgeo/geocube-ingester/service"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service/log"
)

const (
	CopernicusPageLimit     = 1000
	CopernicusQueryURL      = "http://catalogue.dataspace.copernicus.eu/resto/api/collections/search.json?"
	Sentinel1QueryURL       = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?"
	Sentinel2QueryURL       = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"
	CopernicusODataQueryURL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
)

type Provider struct {
	Limit int
}

func (p *Provider) Supports(c common.Constellation) bool {
	switch c {
	case common.Sentinel1, common.Sentinel2:
		return true
	}
	return false
}

func (s *Provider) OpenSearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	// Construct Query
	hostUrl := CopernicusQueryURL
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		hostUrl = Sentinel1QueryURL
	case common.Sentinel2:
		hostUrl = Sentinel2QueryURL
	}
	query, err := opensearch.ConstructQuery(ctx, area, aoi, hostUrl)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("Copernicus.%w", err)
	}

	// Execute query
	rawscenes, err := opensearch.Query(ctx, query, opensearch.Config{Provider: "Copernicus", BaseUrl: hostUrl}, area.Page, area.Limit, s.Limit)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("Copernicus.%w", err)
	}

	// Parse results
	scenes, err := opensearch.Parse(area, rawscenes)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("Copernicus.%w", err)
	}
	return scenes, nil
}

func (s *Provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	if s.Limit == 0 {
		s.Limit = CopernicusPageLimit
	}
	// Construct Query
	mapKey := map[string]string{
		"platformname":          "Collection/Name eq '%s'",
		"producttype":           "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '%s')",
		"polarisationmode":      "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'polarisationChannels' and att/OData.CSC.StringAttribute/Value eq '%s')",
		"sensoroperationalmode": "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq '%s')",
		"cloudcoverpercentage":  "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value ge %s) and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le %s)",
		"relativeorbitnumber":   "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'relativeOrbitNumber' and att/OData.CSC.IntegerAttribute/Value eq %s)",
		"filename":              "contains(Name,'%s')",
	}

	// Default values
	parametersMap := map[string]string{}
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case common.Sentinel1:
		parametersMap[mapKey["platformname"]] = "SENTINEL-1"
		parametersMap[mapKey["producttype"]] = "SLC"
		parametersMap[mapKey["polarisationmode"]] = "VV VH"
		parametersMap[mapKey["sensoroperationalmode"]] = "IW"
	case common.Sentinel2:
		parametersMap[mapKey["platformname"]] = "SENTINEL-2"
		parametersMap[mapKey["producttype"]] = "S2MSI1C"
	default:
		return entities.Scenes{}, fmt.Errorf("Copernicus: constellation not supported: " + area.SceneType.Constellation)
	}

	// Append user-defined parameters
	for k, v := range area.SceneType.Parameters {
		if nk, ok := mapKey[k]; ok {
			k = nk
		}
		parametersMap[k] = v
	}

	// Create query
	var parameters []string
	{
		aoiWKT, err := aoi.ToWKT()
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("Copernicus.searchScenes.ToWKT: %w", err)
		}

		parameters = append(parameters, "OData.CSC.Intersects(area=geography'SRID=4326;"+aoiWKT+"')")
	}

	// Append time
	{
		startDate := area.StartTime.Format("2006-01-02T15:04:05.999Z")
		endDate := area.EndTime.Format("2006-01-02T15:04:05.999Z")
		parameters = append(parameters,
			fmt.Sprintf("ContentDate/Start gt %s", startDate),
			fmt.Sprintf("ContentDate/Start lt %s", endDate))
	}

	for k, v := range parametersMap {
		if strings.Contains(k, "polarisation") {
			v = strings.Replace(v, " ", "&", 1)
		} else if strings.Contains(k, "contains(Name") {
			v = strings.Trim(v, "*")
		} else if strings.Contains(k, "cloudCover") {
			vs := strings.Split(strings.Trim(v[1:], "]"), " TO ")
			if len(vs) != 2 {
				return entities.Scenes{}, fmt.Errorf("Copernicus.searchScenes: cloudcoverpercentage must be 'Min TO Max'")
			}
			parameters = append(parameters, fmt.Sprintf(k, vs[0], vs[1]))
			continue
		}
		parameters = append(parameters, fmt.Sprintf(k, v))
	}
	query := strings.Join(parameters, " and ")

	// Execute query
	rawscenes, err := s.queryCopernicus(ctx, CopernicusODataQueryURL, query, area.Page, area.Limit)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("Copernicus.searchScenes.%w", err)
	}

	// Parse results
	scenes := make([]*entities.Scene, len(rawscenes))
	for i, rawscene := range rawscenes {
		// Check for required elements
		requiredAttributes := []string{"relativeOrbitNumber", "orbitNumber", "productType"}
		for _, attr := range requiredAttributes {
			if _, ok := rawscene.AttributesMap[attr]; !ok {
				return entities.Scenes{}, fmt.Errorf("Copernicus.searchScenes: Missing attribute " + attr + " in results")
			}
		}

		// Parse date
		date, err := time.Parse(time.RFC3339Nano, rawscene.ContentDate.BeginPosition)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("Copernicus.searchScenes.TimeParse: %w", err)
		}
		sourceID := strings.TrimSuffix(rawscene.Identifier, ".SAFE")

		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: sourceID,
				Data: common.SceneAttrs{
					UUID:         rawscene.Uuid,
					Date:         date,
					TileMappings: map[string]common.TileMapping{},
				},
			},
			Tags: map[string]string{
				common.TagSourceID:       sourceID,
				common.TagUUID:           rawscene.Uuid,
				common.TagIngestionDate:  rawscene.ContentDate.BeginPosition,
				common.TagOrbitDirection: rawscene.AttributesMap["orbitDirection"],
				common.TagRelativeOrbit:  rawscene.AttributesMap["relativeOrbitNumber"],
				common.TagOrbit:          rawscene.AttributesMap["orbitNumber"],
				common.TagProductType:    rawscene.AttributesMap["productType"],
			},
			GeometryWKT: wkt.MustEncode(rawscene.Footprint.Geometry),
		}

		// Autofill some fields
		scenes[i].AutoFill()

		// Optional tags
		switch entities.GetConstellation(area.SceneType.Constellation) {
		case common.Sentinel1:
			scenes[i].Tags[common.TagPolarisationMode] = rawscene.AttributesMap["polarisationChannels"]
			scenes[i].Tags[common.TagSliceNumber] = rawscene.AttributesMap["sliceNumber"]
		case common.Sentinel2:
			scenes[i].Tags[common.TagCloudCoverPercentage] = rawscene.AttributesMap["cloudCover"]
		}
	}

	return entities.Scenes{
		Scenes:     scenes,
		Properties: nil,
	}, nil
}

type Hits struct {
	Uuid        string           `json:"Id"`
	Identifier  string           `json:"Name"`
	Footprint   geojson.Geometry `json:"GeoFootprint"`
	ContentDate struct {
		BeginPosition string `json:"Start"`
	} `json:"ContentDate"`
	Attributes []struct {
		Name      string      `json:"Name"`
		Value     interface{} `json:"Value"`
		ValueType string      `json:"ValueType"`
	} `json:"Attributes"`
	AttributesMap map[string]string
}

func (s *Provider) queryCopernicus(ctx context.Context, baseurl, query string, page, limit int) ([]Hits, error) {
	// Pagging
	var rawscenes []Hits
	query = neturl.QueryEscape(query)
	totalPages, count := "?", false // count is false: it takes too much time... It can be set to true for debugging purpose

	for _, queryParams := range service.ComputePagesToQuery(page, limit, s.Limit) {
		log.Logger(ctx).Sugar().Debugf("[Copernicus] Search page %d/%s", queryParams.Page+1, totalPages)
		// Load results
		url := baseurl + query + fmt.Sprintf("&$orderby=ContentDate/Start&$top=%d&$skip=%d&$expand=Attributes", queryParams.Limit, queryParams.Limit*queryParams.Page)
		if count {
			url += "&$count=True"
		}
		jsonResults, err := service.GetBodyRetry(url, 3)
		if err != nil {
			return nil, fmt.Errorf("queryCopernicus: %w", err)
		}

		//JSON
		results := struct {
			Status int    `json:"status"`
			Next   string `json:"@odata.nextLink"`
			Count  int    `json:"@odata.count"`
			Hits   []Hits `json:"value"`
		}{}

		// Read results to retrieve scenes
		if err := json.Unmarshal(jsonResults, &results); err != nil {
			return nil, fmt.Errorf("query.Unmarshal : %w (response: %s)", err, jsonResults)
		}

		if results.Status != 0 && results.Status != 200 {
			return nil, fmt.Errorf("query: http status: %d (response: %s)", results.Status, jsonResults)
		}

		results.Hits = service.QueryGetResult(&queryParams, results.Hits)

		for i, hit := range results.Hits {
			results.Hits[i].AttributesMap = map[string]string{}
			for _, elem := range hit.Attributes {
				switch e := elem.Value.(type) {
				default:
					results.Hits[i].AttributesMap[elem.Name] = fmt.Sprintf("%v", e)
				}
			}
			results.Hits[i].Attributes = nil
		}

		// Merge the results
		rawscenes = append(rawscenes, results.Hits...)

		// Is there a next page ?
		if results.Next == "" || len(rawscenes) == limit {
			break
		}
		if results.Count > 0 {
			totalPages = strconv.Itoa((results.Count-1)/queryParams.Limit + 1)
			count = false
		}
	}

	return rawscenes, nil
}
