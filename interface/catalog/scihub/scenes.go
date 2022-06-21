package scihub

import (
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/service/log"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

type Provider struct {
	Username string
	Password string
}

func (s *Provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) ([]*entities.Scene, error) {
	// Construct Query
	var parameters []string
	{
		convexhull, err := aoi.ConvexHull()
		if err != nil {
			return nil, fmt.Errorf("Scihub.searchScenes.ConvexHull: %w", err)
		}

		convexhullWKT, err := convexhull.ToWKT()
		if err != nil {
			return nil, fmt.Errorf("Scihub.searchScenes.ToWKT: %w", err)
		}

		parameters = append(parameters, "( footprint:\"Intersects("+convexhullWKT+")\")")
	}

	// Append time
	{
		startDate := area.StartTime.Format("2006-01-02") + "T00:00:00.000Z"
		endDate := area.EndTime.Format("2006-01-02") + "T23:59:59.999Z"
		parameters = append(parameters,
			fmt.Sprintf("(beginPosition:[ %s TO %s ] )", startDate, endDate),
			fmt.Sprintf("(endPosition:[ %s TO %s ] )", startDate, endDate))
	}

	// Append parameters
	for k, v := range area.SceneType.Parameters {
		area.SceneType.Parameters[k] = v
	}
	switch entities.GetConstellation(area.SceneType.Constellation) {
	case entities.Sentinel1:
		area.SceneType.Parameters["platformname"] = "Sentinel-1"
		// Default values
		if _, ok := area.SceneType.Parameters["producttype"]; !ok {
			area.SceneType.Parameters["producttype"] = "SLC"
		}
		if _, ok := area.SceneType.Parameters["polarisationmode"]; !ok {
			area.SceneType.Parameters["polarisationmode"] = "VV VH"
		}
		if _, ok := area.SceneType.Parameters["sensoroperationalmode"]; !ok {
			area.SceneType.Parameters["sensoroperationalmode"] = "IW"
		}
	case entities.Sentinel2:
		area.SceneType.Parameters["platformname"] = "Sentinel-2"
		// Default values
		if _, ok := area.SceneType.Parameters["producttype"]; !ok {
			area.SceneType.Parameters["producttype"] = "S2MSI1C"
		}
	default:
		return nil, fmt.Errorf("constellation not supported: " + area.SceneType.Constellation)
	}

	for k, v := range area.SceneType.Parameters {
		parameters = append(parameters, fmt.Sprintf("( %s:%s )", k, v))
	}

	query := "(" + strings.Join(parameters, " AND ") + ")"

	// Execute query
	rawscenes, err := s.queryScihub(ctx, "https://apihub.copernicus.eu/apihub/search?q=", query)
	if err != nil {
		log.Logger(ctx).Sugar().Debugf("apihub failed with error : %v. Trying dhus instead", err)
		if rawscenes, err = s.queryScihub(ctx, "https://scihub.copernicus.eu/dhus/search?q=", query); err != nil {
			return nil, fmt.Errorf("Scihub.searchScenes.%w", err)
		}
	}

	// Parse results
	scenes := make([]*entities.Scene, len(rawscenes))
	for i, rawscene := range rawscenes {
		// Check for required elements
		requiredElements := []string{"platformname", "identifier", "beginposition", "uuid", "ingestiondate", "orbitdirection", "relativeorbitnumber", "orbitnumber", "producttype", "footprint"}
		for _, elem := range requiredElements {
			if _, ok := rawscene[elem]; !ok {
				return nil, fmt.Errorf("Scihub.searchScenes: Missing element " + elem + " in results")
			}
		}

		// Parse date
		date, err := time.Parse(time.RFC3339Nano, rawscene["beginposition"])
		if err != nil {
			return nil, fmt.Errorf("Scihub.searchScenes.TimeParse: %w", err)
		}

		// Parse aoi
		wktAOI := strings.ToUpper(rawscene["footprint"])
		if _, err := wkt.DecodeString(wktAOI); err != nil {
			return nil, fmt.Errorf("Scihub.searchScenes.wktDecodeString[%s]: %w", wktAOI, err)
		}

		// Create scene
		scenes[i] = &entities.Scene{
			Scene: common.Scene{
				SourceID: rawscene["identifier"],
				AOI:      area.AOIID,
				Data: common.SceneAttrs{
					UUID:         rawscene["uuid"],
					Date:         date,
					GraphName:    area.SceneGraphName,
					GraphConfig:  area.GraphConfig,
					TileMappings: map[string]common.TileMapping{},
				},
			},
			Tags: map[string]string{
				common.TagSourceID:       rawscene["identifier"],
				common.TagUUID:           rawscene["uuid"],
				common.TagIngestionDate:  rawscene["ingestiondate"],
				common.TagOrbitDirection: rawscene["orbitdirection"],
				common.TagRelativeOrbit:  rawscene["relativeorbitnumber"],
				common.TagOrbit:          rawscene["orbitnumber"],
				common.TagProductType:    rawscene["producttype"],
			},
			GeometryWKT: wktAOI,
		}

		// Autofill some fields
		scenes[i].AutoFill()

		// Optional tags
		switch entities.GetConstellation(area.SceneType.Constellation) {
		case entities.Sentinel1:
			scenes[i].Tags[common.TagPolarisationMode] = rawscene["polarisationmode"]
			scenes[i].Tags[common.TagSliceNumber] = rawscene["slicenumber"]
			scenes[i].Tags[common.TagLastOrbit] = rawscene["lastorbitnumber"]
			scenes[i].Tags[common.TagLastRelativeOrbit] = rawscene["lastrelativeorbitnumber"]
		}

		// Copy area tags
		for k, v := range area.RecordTags {
			if _, ok := scenes[i].Tags[k]; !ok {
				scenes[i].Tags[k] = v
			}
		}
	}

	return scenes, nil
}

func (s *Provider) queryScihub(ctx context.Context, baseurl, query string) ([]map[string]string, error) {
	// Pagging
	var rawscenes []map[string]string
	nextPage := true
	query = neturl.QueryEscape(query)
	totalPages := "?"
	for index, rows := 0, 100; nextPage; index += rows {
		log.Logger(ctx).Sugar().Debugf("Search page %d/%s", index/rows+1, totalPages)
		// Load results
		var xmlResults []byte
		{
			url := baseurl + query + fmt.Sprintf("&rows=%d&start=%d", rows, index)
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				return nil, fmt.Errorf("queryScihub.NewRequest: %w", err)
			}
			req = req.WithContext(ctx)
			req.SetBasicAuth(s.Username, s.Password)

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				return nil, fmt.Errorf("queryScihub.Req: %w", err)
			}
			defer resp.Body.Close()

			if xmlResults, err = ioutil.ReadAll(resp.Body); err != nil {
				return nil, fmt.Errorf("queryScihub.ReadAll: %w", err)
			}
		}

		// XML Element structure:
		type Element struct {
			Name  string `xml:"name,attr"`
			Value string `xml:",chardata"`
		}

		// Read results to retrieve scenes
		results := struct {
			XMLName xml.Name `xml:"feed"`
			Error   struct {
				Code    string `xml:"code"`
				Message string `xml:"message"`
			} `xml:"error"`
			Entries []struct {
				StrElements  []Element `xml:"str"`
				IntElements  []Element `xml:"int"`
				DateElements []Element `xml:"date"`
			} `xml:"entry"`
			Links []struct {
				Rel  string `xml:"rel,attr"`
				Href string `xml:"href,attr"`
			} `xml:"link"`
			TotalResults int `xml:"totalResults"`
		}{}
		if err := xml.Unmarshal(xmlResults, &results); err != nil {
			return nil, fmt.Errorf("queryScihub.Unmarshal : %w (response: %s)", err, xmlResults)
		}
		if results.Error.Code != "" {
			return nil, fmt.Errorf("queryScihub : %s[code:%s]", results.Error.Message, results.Error.Code)
		}

		// Merge all elements of the scene into a dict
		for _, entry := range results.Entries {
			rawscene := map[string]string{}
			for _, elem := range entry.StrElements {
				rawscene[elem.Name] = elem.Value
			}
			for _, elem := range entry.IntElements {
				rawscene[elem.Name] = elem.Value
			}
			for _, elem := range entry.DateElements {
				rawscene[elem.Name] = elem.Value
			}
			rawscenes = append(rawscenes, rawscene)
		}

		// Is there a next page ?
		nextPage = false
		for _, link := range results.Links {
			if strings.ToLower(link.Rel) == "next" && link.Href != "" {
				nextPage = true
			}
		}
		if results.TotalResults != 0 {
			totalPages = strconv.Itoa(results.TotalResults/rows + 1)
		}
	}

	return rawscenes, nil
}
