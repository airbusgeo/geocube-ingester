package oneatlas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"maps"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/shared"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/geometry"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/araddon/dateparse"
	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/geojson"
	"github.com/go-spatial/geom/encoding/wkt"
	"github.com/paulsmith/gogeos/geos"
)

const (
	OneAtlasCatalogLimit           = 200
	OneAtlasCreateApiKeyEndpoint   = "https://account.foundation.oneatlas.airbus.com/api-keys"
	OneAtlasAuthenticationEndpoint = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
	OneAtlasOrderEndpoint          = "https://data.api.oneatlas.airbus.com"
	OneAtlasSearchEndpoint         = "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch"
)

type provider struct {
	username       string
	password       string
	searchEndpoint string
	orderManager   shared.OrderManager
	limit          int
}

func NewOneAtlasProvider(ctx context.Context, username, apikey, searchEndpoint, orderEndpoint, authenticationEndpoint string) (*provider, context.CancelFunc) {
	orderManager, cncl := shared.NewOrderManager(ctx, orderEndpoint, authenticationEndpoint, apikey)
	return &provider{
		username:       username,
		password:       apikey,
		searchEndpoint: searchEndpoint,
		orderManager:   orderManager,
		limit:          OneAtlasCatalogLimit,
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
		return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas].%w", err)
	}

	catalogResponse, err := p.queryCatalog(ctx, catalogRequestParameter, area.Page, area.Limit)
	if err != nil {
		return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas].%w", err)
	}

	scenes := make([]*entities.Scene, 0)
	orderProducts := make([]shared.Product, 0)
	for _, feature := range catalogResponse.Features {
		uuid, ok := feature.Properties["id"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to parse id: %v", feature.Properties["id"])
		}

		var identifier string
		for _, elem := range []string{"sourceIdentifier", "acquisitionIdentifier", "parentIdentifier"} {
			if identifier, ok = feature.Properties[elem].(string); ok {
				break
			}
		}
		if identifier == "" {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to find identifier")
		}
		parentIdentifier, ok := feature.Properties["parentIdentifier"].(string)
		if !ok {
			parentIdentifier = identifier
		}

		processingDateStr, ok := feature.Properties["processingDate"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to parse processingDate: %v", feature.Properties["processingDate"])
		}
		processingDate, err := dateparse.ParseAny(processingDateStr)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed parse processingDate as time: %s", processingDateStr)
		}

		acquisitionDateStr, ok := feature.Properties["acquisitionDate"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to parse acquisitionDate: %v", feature.Properties["acquisitionDate"])
		}

		acquisitionDate, err := dateparse.ParseAny(acquisitionDateStr)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed parse acquisitionDate as time: %s", acquisitionDateStr)
		}

		productType, ok := feature.Properties["productType"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to parse productType: %v", feature.Properties["productType"])
		}

		constellation, ok := feature.Properties["constellation"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to parse constellation: %v", feature.Properties["constellation"])
		}

		platform, ok := feature.Properties["platform"].(string)
		if !ok {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to parse platform: %v", feature.Properties["platform"])
		}

		cloudcover := fmt.Sprintf("%v", feature.Properties["cloudCover"])

		if p.isSceneAlreadyAdded(identifier, identifier, scenes) {
			continue
		}

		intersectGeometry, err := p.getIntersectGeometry(feature.Geometry.Geometry, aoi)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to get geometry intersection: %w", err)
		}
		if empty, err := intersectGeometry.IsEmpty(); err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to get geometry intersection: %w", err)
		} else if empty {
			log.Logger(ctx).Sugar().Debugf("Empty intersection with %s", identifier)
			continue
		}
		if area, err := intersectGeometry.Area(); err != nil {
			return entities.Scenes{}, fmt.Errorf("SearchScenes[OneAtlas]: failed to get geometry intersection: %w", err)
		} else if area < 0.00001 {
			log.Logger(ctx).Sugar().Debugf("Too small intersection with %s (area=%fdeg²)", identifier, area)
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
				break
			}
		}

		g, err := geometry.GeosToGeom(intersectGeometry)
		if err != nil {
			return entities.Scenes{}, fmt.Errorf("failed to compute geometry from intersect geometry: %w", err)
		}
		metadata := map[string]interface{}{
			common.DownloadLinkMetadata: downloadURL,
			"geometry":                  &geojson.Geometry{Geometry: g},
			common.UUIDMetadata:         uuid,
		}
		maps.Copy(metadata, feature.Properties)

		newScene := &entities.Scene{
			Scene: common.Scene{
				SourceID: parentIdentifier,
				Data: common.SceneAttrs{
					Date:         processingDate,
					TileMappings: map[string]common.TileMapping{},
					Metadata:     metadata,
				},
			},
			ProductName: identifier,
			Tags: map[string]string{
				common.TagSourceID:             identifier,
				common.TagUUID:                 uuid,
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
				ID:                    uuid,
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
					uuid, ok := scene.Scene.Data.Metadata[common.UUIDMetadata].(string)
					if !ok {
						return entities.Scenes{}, fmt.Errorf("failed to get scene uuid in metadata for %s", scene.SourceID)
					}
					if strings.EqualFold(d.ID, uuid) {
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
				uuid, ok := scene.Scene.Data.Metadata[common.UUIDMetadata].(string)
				if !ok {
					return entities.Scenes{}, fmt.Errorf("failed to get scene uuid in metadata for %s", scene.SourceID)
				}
				sceneOrderStatus, ok := orderStatus[uuid]
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
					if strings.EqualFold(d.ID, uuid) {
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
	constellation := common.GetConstellationFromString(area.SceneType.Constellation)
	if !p.Supports(constellation) {
		return nil, fmt.Errorf("OneAtlas: constellation not supported: %s", area.SceneType.Constellation)
	}
	convexHullGeometry, err := aoi.ConvexHull()
	if err != nil {
		return nil, err
	}

	g, err := geometry.GeosToGeom(convexHullGeometry)
	if err != nil {
		return nil, err
	}

	var acquisitionDate string
	{
		startDate := area.StartTime.Format("2006-01-02") + "T00:00:00.000Z"
		endDate := area.EndTime.Format("2006-01-02") + "T23:59:59.999Z"
		acquisitionDate = fmt.Sprintf("[%s,%s]", startDate, endDate)
	}

	productType := area.SceneType.Parameters["productType"]
	platform := area.SceneType.Parameters["platform"]
	processingLevel := area.SceneType.Parameters["processingLevel"]
	cloudCover := strings.ReplaceAll(area.SceneType.Parameters["cloudCover"], " ", "")
	incidenceAngle := area.SceneType.Parameters["incidenceAngle"]
	workspace := area.SceneType.Parameters["workspace"]
	relation := area.SceneType.Parameters["relation"]
	acquisitionIdentifier := area.SceneType.Parameters["acquisitionIdentifier"]

	return &catalogRequestParameter{
		Constellation:         constellation.String(),
		AcquisitionIdentifier: acquisitionIdentifier,
		ItemsPerPage:          area.Limit,
		StartPage:             area.Page + 1,
		ProcessingLevel:       processingLevel,
		ProductType:           productType,
		SortBy:                "acquisitionDate",
		AcquisitionDate:       acquisitionDate,
		Platform:              platform,
		CloudCover:            cloudCover,
		IncidenceAngle:        incidenceAngle,
		Workspace:             workspace,
		Relation:              relation,
		Geometry:              geojson.Geometry{Geometry: g},
	}, nil
}

func (p *provider) queryCatalog(ctx context.Context, catalogRequestParameter *catalogRequestParameter, page, limit int) (*catalogResponse, error) {
	var completeCatalogResponse catalogResponse
	totalPages := "?"

	for _, queryParams := range service.ComputePagesToQuery(page, limit, p.limit) {
		log.Logger(ctx).Sugar().Debugf("[OneAtlas] Search page %d/%s", queryParams.Page+1, totalPages)
		catalogRequestParameter.StartPage = queryParams.Page + 1
		catalogRequestParameter.ItemsPerPage = queryParams.Limit
		b, err := json.Marshal(catalogRequestParameter)
		if err != nil {
			return nil, fmt.Errorf("queryCatalog: %w", err)
		}

		request, err := http.NewRequest(http.MethodPost, p.searchEndpoint, bytes.NewReader(b))
		if err != nil {
			return nil, fmt.Errorf("queryCatalog: %w", err)
		}

		request.Header.Add("Content-Type", "application/json")
		request.SetBasicAuth(p.username, p.password)

		bodyResponse, err := service.GetBodyRetryReq(request, 3)
		if err != nil {
			return nil, fmt.Errorf("queryCatalog.%w", err)
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
		c.Features = service.QueryGetResult(&queryParams, c.Features)
		completeCatalogResponse.Features = append(completeCatalogResponse.Features, c.Features...)

		nbPages := c.TotalResults/c.ItemsPerPage + 1
		totalPages = strconv.Itoa(nbPages)
		if catalogRequestParameter.StartPage == nbPages || len(completeCatalogResponse.Features) == limit {
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

func (p *provider) getIntersectGeometry(g geom.Geometry, aoi geos.Geometry) (*geos.Geometry, error) {
	wkt, err := wkt.EncodeString(g)
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
