package oneatlas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/shared"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/araddon/dateparse"
	"github.com/paulsmith/gogeos/geos"
)

type provider struct {
	username     string
	password     string
	endpoint     string
	client       *http.Client
	orderManager shared.OrderManager
}

func NewOneAtlasProvider(ctx context.Context, username, apikey, endpoint, orderEndpoint, authenticationEndpoint string) (*provider, context.CancelFunc) {
	orderManager, cncl := shared.NewOrderManager(ctx, orderEndpoint, authenticationEndpoint, apikey)
	return &provider{
		username:     username,
		password:     apikey,
		endpoint:     endpoint,
		client:       &http.Client{},
		orderManager: orderManager,
	}, cncl
}
func (p *provider) Supports(c common.Constellation) bool {
	switch c {
	case common.PHR, common.SPOT:
		return true
	}
	return false
}

func (p *provider) SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error) {
	catalogRequestParameter, err := p.buildCatalogParameters(area, aoi)
	if err != nil {
		return entities.Scenes{}, err
	}

	catalogResponse, err := p.queryCatalog(ctx, catalogRequestParameter)
	if err != nil {
		return entities.Scenes{}, err
	}

	scenes := make([]*entities.Scene, 0)
	orderProducts := make([]shared.Product, 0)
	for _, feature := range catalogResponse.Features {
		// Check for required elements
		requiredElements := []string{"platform", "processingDate", "parentIdentifier", "acquisitionIdentifier", "id", "acquisitionDate", "productType"}
		for _, elem := range requiredElements {
			if _, ok := feature.Properties[elem]; !ok {
				return entities.Scenes{}, fmt.Errorf("OneAtlas.searchScenes: Missing element " + elem + " in results")
			}
		}

		id, ok := feature.Properties["id"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse id: %v", feature.Properties["id"])
		}

		parentIdentifier, ok := feature.Properties["parentIdentifier"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse parentIdentifier: %v", feature.Properties["parentIdentifier"])
		}

		acquisitionIdentifier, ok := feature.Properties["acquisitionIdentifier"].(string)
		if !ok {
			acquisitionIdentifier, ok = feature.Properties["parentIdentifier"].(string)
			if !ok {
				return entities.Scenes{}, fmt.Errorf("failed to parse acquisitionIdentifier: %v", feature.Properties["acquisitionIdentifier"])
			}
		}

		processingDateStr, ok := feature.Properties["processingDate"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse processingDate: %v", feature.Properties["processingDate"])
		}
		processingDate, err := dateparse.ParseAny(processingDateStr)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed parse processingDate as time")
		}

		acquisitionDateStr, ok := feature.Properties["acquisitionDate"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse acquisitionDate: %v", feature.Properties["acquisitionDate"])
		}

		acquisitionDate, err := dateparse.ParseAny(acquisitionDateStr)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed parse acquisitionDate as time")
		}

		productType, ok := feature.Properties["productType"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse productType: %v", feature.Properties["productType"])
		}

		constellation, ok := feature.Properties["constellation"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse constellation: %v", feature.Properties["constellation"])
		}

		platform, ok := feature.Properties["platform"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("failed to parse platform: %v", feature.Properties["platform"])
		}

		cloudcover, _ := feature.Properties["cloudcover"].(string)

		if p.isSceneAlreadyAdded(parentIdentifier, acquisitionIdentifier, scenes) {
			continue
		}

		intersectGeometry, err := p.GetIntersectGeometry(feature.Geometry, aoi)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to get geometry intersection: %w", err)
		}
		if empty, err := intersectGeometry.IsEmpty(); err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to get geometry intersection: %w", err)
		} else if empty {
			log.Logger(ctx).Sugar().Debugf("Empty intersection with %s", acquisitionIdentifier)
			continue
		}
		if area, err := intersectGeometry.Area(); err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to get geometry intersection: %w", err)
		} else if area < 0.00001 {
			log.Logger(ctx).Sugar().Debugf("Too small intersection with %s (area=%fdeg²)", acquisitionIdentifier, area)
			continue
		}

		wktGeometry, err := intersectGeometry.ToWKT()
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to extract geometry in WKT: %w", err)
		}

		downloadURL := ""
		downloadLinks := feature.Links.GetLinks("download")
		for _, link := range downloadLinks {
			if strings.EqualFold(link.Name, "download") && strings.EqualFold(link.Type, "http") {
				downloadURL = link.Href
			}
		}

		metadata := make(map[string]interface{})
		metadata[common.DownloadLinkMetadata] = downloadURL
		g, err := p.computeGeometryFromAOI(*intersectGeometry)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to compute geometry from intersect geometry: %w", err)
		}

		metadata["geometry"] = *g
		for featureKey, featureValue := range feature.Properties {
			metadata[featureKey] = featureValue
		}

		newScene := &entities.Scene{
			Scene: common.Scene{
				SourceID: parentIdentifier,
				Data: common.SceneAttrs{
					UUID:         id,
					Date:         processingDate,
					TileMappings: map[string]common.TileMapping{},
					Metadata:     metadata,
				},
			},
			ProductName: acquisitionIdentifier,
			Tags: map[string]string{
				common.TagSourceID:             parentIdentifier,
				common.TagUUID:                 id,
				common.TagIngestionDate:        acquisitionDate.String(),
				common.TagProductType:          productType,
				common.TagConstellation:        constellation,
				common.TagSatellite:            platform,
				common.TagCloudCoverPercentage: cloudcover,
			},
			GeometryWKT: wktGeometry,
		}

		// Order Needed
		if downloadURL == "" {
			aoi, err := shared.GetAoiFromGeometry(newScene.Scene)
			if err != nil {
				return entities.Scenes{}, fmt.Errorf("failed convert geometry to aoi: %w", err)
			}

			orderProducts = append(orderProducts, shared.Product{
				CrsCode:               "urn:ogc:def:crs:EPSG::4326",
				ProductType:           productType,
				RadiometricProcessing: "BASIC16",
				Aoi:                   aoi,
				ID:                    newScene.Data.UUID,
				ImageFormat:           "image/geotiff",
			})
		}
		scenes = append(scenes, newScene)
	}

	scenesResult := entities.Scenes{
		Scenes:     scenes,
		Properties: map[string]string{},
	}

	if getPrice, ok := area.SceneType.Parameters["GetPrice"]; ok && strings.ToLower(getPrice) == "true" {
		log.Logger(ctx).Sugar().Debugf("Check price")
		orderRequest := shared.OrderRequest{
			Kind:     "order.data.product",
			Products: orderProducts,
		}

		price, err := p.orderManager.GetPrice(orderRequest)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to check price before start order: %w", err)
		}

		var globalAmount int
		amountUnit := price.AmountUnit
		orderStatus, err := p.orderManager.GetStatus(orderRequest)
		switch {
		case err == shared.ErrOrderNotFound:
			for _, scene := range scenes {
				for _, d := range price.Deliveries {
					if strings.EqualFold(d.ID, scene.Scene.Data.UUID) {
						globalAmount += d.Amount
						scene.Tags["EstimatedCost"] = fmt.Sprintf("%d", d.Amount)
						scene.Tags["amountUnit"] = d.AmountUnit
						continue
					}
				}
			}

		case err != nil:
			return entities.Scenes{}, fmt.Errorf("failed to get status order: %w", err)
		default:
			for _, scene := range scenes {
				sceneOrderStatus, ok := orderStatus[scene.Scene.Data.UUID]
				if ok {
					if strings.EqualFold(sceneOrderStatus.State, "delivered") {
						if scene.Data.Metadata == nil {
							scene.Data.Metadata = map[string]any{}
						}
						scene.Data.Metadata[common.DownloadLinkMetadata] = sceneOrderStatus.DownloadLink
					}
					continue
				}

				for _, d := range price.Deliveries {
					if strings.EqualFold(d.ID, scene.Scene.Data.UUID) {
						globalAmount += d.Amount
						scene.Tags["estimatedCost"] = fmt.Sprintf("%d", d.Amount)
						scene.Tags["amountUnit"] = d.AmountUnit
					}
				}
			}
		}

		scenesResult.Properties["globalEstimatedCost"] = fmt.Sprintf("%d", globalAmount)
		scenesResult.Properties["amountUnit"] = amountUnit
	}

	return scenesResult, nil
}

func (p *provider) buildCatalogParameters(area *entities.AreaToIngest, aoi geos.Geometry) (*catalogRequestParameter, error) {
	g, err := p.computeGeometryFromAOI(aoi)
	if err != nil {
		return nil, err
	}

	var acquisitionDate string
	{
		startDate := area.StartTime.Format("2006-01-02") + "T00:00:00.000Z"
		endDate := area.EndTime.Format("2006-01-02") + "T23:59:59.999Z"
		acquisitionDate = fmt.Sprintf("[%s,%s]", startDate, endDate)
	}

	constellation := entities.GetConstellation(area.SceneType.Constellation)
	if constellation != common.PHR && constellation != common.SPOT {
		return nil, fmt.Errorf("OneAtlas: constellation not supported: " + area.SceneType.Constellation)
	}

	productType := area.SceneType.Parameters["productType"]
	platform := area.SceneType.Parameters["platform"]
	processingLevel := area.SceneType.Parameters["processingLevel"]
	cloudCover := area.SceneType.Parameters["cloudCover"]
	incidenceAngle := area.SceneType.Parameters["incidenceAngle"]
	workspace := area.SceneType.Parameters["workspace"]
	relation := area.SceneType.Parameters["relation"]

	return &catalogRequestParameter{
		Constellation:   constellation.String(),
		ItemsPerPage:    200,
		StartPage:       1,
		ProcessingLevel: processingLevel,
		ProductType:     productType,
		SortBy:          "acquisitionDate",
		AcquisitionDate: acquisitionDate,
		Platform:        platform,
		CloudCover:      cloudCover,
		IncidenceAngle:  incidenceAngle,
		Workspace:       workspace,
		Relation:        relation,
		Geometry:        g,
	}, nil
}

func (p *provider) queryCatalog(_ context.Context, catalogRequestParameter *catalogRequestParameter) (*catalogResponse, error) {
	var completeCatalogResponse catalogResponse

	nbPage := 0
	for {
		b, err := json.Marshal(catalogRequestParameter)
		if err != nil {
			return nil, err
		}

		request, err := http.NewRequest(http.MethodPost, p.endpoint, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}

		request.Header.Add("Content-Type", "application/json")
		request.SetBasicAuth(p.username, p.password)

		resp, err := p.client.Do(request)
		if err != nil {
			return nil, err
		}

		//noinspection GoDeferInLoop
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return nil, fmt.Errorf(resp.Status)
			}
			return nil, service.MakeTemporary(fmt.Errorf(resp.Status))
		}

		bodyResponse, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		c := catalogResponse{}
		if err = json.Unmarshal(bodyResponse, &c); err != nil {
			return nil, err
		}

		completeCatalogResponse.Error = c.Error
		completeCatalogResponse.ItemsPerPage = c.ItemsPerPage
		completeCatalogResponse.StartIndex = c.StartIndex
		completeCatalogResponse.TotalResults = c.TotalResults
		completeCatalogResponse.Type = c.Type
		completeCatalogResponse.Features = append(completeCatalogResponse.Features, c.Features...)

		if c.ItemsPerPage != 0 {
			nbPage = c.TotalResults/c.ItemsPerPage + 1
		}
		if catalogRequestParameter.StartPage < nbPage {
			catalogRequestParameter.StartPage++
		} else {
			break
		}
	}

	return &completeCatalogResponse, nil

}

func (p *provider) isSceneAlreadyAdded(sourceID, productName string, sceneList []*entities.Scene) bool {
	for _, scene := range sceneList {
		if strings.EqualFold(scene.SourceID, sourceID) && strings.EqualFold(scene.ProductName, productName) {
			return true
		}
	}
	return false
}

func (p *provider) computeGeometryFromAOI(g geos.Geometry) (*geometry, error) {
	convexHullGeometry, err := g.ConvexHull()
	if err != nil {
		return nil, err
	}

	// get number of polygons
	numPolygons, err := convexHullGeometry.NGeometry()
	if err != nil {
		return nil, err
	}

	if numPolygons != 1 {
		return nil, fmt.Errorf("multiple polygon is not supported")
	}

	coords := make([][][2]float64, 0)
	polygon, err := convexHullGeometry.Geometry(0)
	if err != nil {
		return nil, err
	}

	// get shell
	shell, err := polygon.Shell()
	if err != nil {
		return nil, err
	}

	// append shell coords
	shellCoords, err := p.getCoords(shell)
	if err != nil {
		return nil, err
	}
	coords = append(coords, shellCoords)

	// get holes
	holes, err := polygon.Holes()
	if err != nil {
		return nil, err
	}

	// append holes coords
	for _, hole := range holes {
		holeCoords, err := p.getCoords(hole)
		if err != nil {
			return nil, err
		}
		coords = append(coords, holeCoords)
	}

	return &geometry{
		Type:        "Polygon",
		Coordinates: coords,
	}, nil
}

func (p *provider) getCoords(ring *geos.Geometry) ([][2]float64, error) {
	// get coords of the ring
	ringCoords, err := ring.Coords()
	if err != nil {
		return nil, err
	}

	// fill coords array
	coords := make([][2]float64, 0)
	for _, coord := range ringCoords {
		coords = append(coords, [2]float64{coord.X, coord.Y})
	}

	return coords, nil
}

func (p *provider) GetIntersectGeometry(g *geometry, aoi geos.Geometry) (*geos.Geometry, error) {
	wkt, err := p.encodeWKTPolygonGeometry(g)
	if err != nil {
		return nil, fmt.Errorf("failed to encode WKT polygon: %w", err)
	}

	featureGeometry, err := geos.FromWKT(wkt)
	if err != nil {
		return nil, fmt.Errorf("failed to create geometry from WKT: %w", err)
	}

	intersectGeom, err := featureGeometry.Intersection(&aoi)
	if err != nil {
		return nil, fmt.Errorf("failed to make intersection between aoi and feature geometry: %w", err)
	}

	return intersectGeom, nil
}

func (p *provider) encodeWKTPolygonGeometry(g *geometry) (string, error) {
	var wkt strings.Builder

	wkt.WriteString("POLYGON(")
	for _, ring := range g.Coordinates {
		wkt.WriteString("(")
		var coords []string
		for _, pt := range ring {
			coords = append(coords, fmt.Sprintf("%f %f", pt[0], pt[1]))
		}
		wkt.WriteString(strings.Join(coords, ","))
		wkt.WriteString(")")
	}
	wkt.WriteString(")")

	return wkt.String(), nil
}
