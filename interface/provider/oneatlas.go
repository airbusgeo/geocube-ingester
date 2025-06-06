package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/shared"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/cavaliercoder/grab"
)

type OneAtlasProvider struct {
	user             string
	password         string
	downloadEndpoint string
	client           *grab.Client
	orderManager     shared.OrderManager
}

func NewOneAtlasProvider(ctx context.Context, user, apikey, downloadEndpoint, orderEndpoint, authenticationEndpoint string) (*OneAtlasProvider, context.CancelFunc) {
	orderManager, cncl := shared.NewOrderManager(ctx, orderEndpoint, authenticationEndpoint, apikey)
	return &OneAtlasProvider{
		user:             user,
		password:         apikey,
		downloadEndpoint: downloadEndpoint,
		client:           grab.NewClient(),
		orderManager:     orderManager,
	}, cncl
}

// Name implements ImageProvider
func (o *OneAtlasProvider) Name() string {
	return "OneAtlas"
}

// Download implements ImageProvider
func (o *OneAtlasProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID

	switch common.GetConstellationFromProductId(sceneName) {
	case common.PHR, common.SPOT:
	default:
		return fmt.Errorf("OneAtlasProvider: constellation not supported: %s", sceneName)
	}
	if scene.Data.Metadata == nil {
		return fmt.Errorf("OneAtlasProvider: unable to retrieve download Link: scene metadata is empty")
	}

	downloadLink, ok := scene.Data.Metadata[common.DownloadLinkMetadata]
	if !ok || downloadLink == "" {
		var err error
		downloadLink, err = o.sendOrderRequest(scene)
		if err != nil {
			return fmt.Errorf("failed to process order request: %w", err)
		}
		if downloadLink == "" {
			return service.MakeTemporary(fmt.Errorf("order is potentially still in progress"))
		}
	}

	downloadLinkURL, ok := downloadLink.(string)
	if !ok {
		return fmt.Errorf("failed to cast download link to string: %v", downloadLinkURL)
	}

	downloadURL, err := url.Parse(downloadLinkURL)
	if err != nil {
		return fmt.Errorf("failed to parse oneAtlas download endpoint: %w", err)
	}

	if err := o.download(ctx, downloadURL.String(), localDir, sceneName, o.Name(), &o.user, &o.password, nil); err != nil {
		return fmt.Errorf("OneAtlasProvider.%w", err)
	}
	return nil
}

func (o *OneAtlasProvider) download(ctx context.Context, downloadURL string, localDir string, sceneName string, _ string, user *string, pwd *string, header http.Header) error {
	localZip := sceneFilePath(localDir, sceneName, service.ExtensionZIP)
	request, err := grab.NewRequest(localZip, downloadURL)
	if err != nil {
		return fmt.Errorf("download.NewRequest: %w", err)
	}
	request = request.WithContext(ctx)

	// If Basic Auth
	if user != nil && pwd != nil {
		request.HTTPRequest.SetBasicAuth(*user, *pwd)
	}

	// If key/val Auth
	if header != nil {
		request.HTTPRequest.Header = header
	}

	response := o.client.Do(request)
	if err := response.Err(); err != nil {
		err = fmt.Errorf("download[%s]: %w", request.URL(), err)
		if response.HTTPResponse == nil {
			return service.MakeTemporary(err)
		}
		switch response.HTTPResponse.StatusCode {
		case 408, 429, 500, 501, 502, 503, 504:
			return service.MakeTemporary(err)
		default:
			return err
		}
	}

	defer os.Remove(localZip)
	if err := unarchive(ctx, localZip, localDir); err != nil {
		return service.MakeTemporary(fmt.Errorf("download.Unarchive: %w", err))
	}

	return nil

}

func (o *OneAtlasProvider) sendOrderRequest(scene common.Scene) (string, error) {
	aoi, err := shared.GetAoiFromGeometry(scene)
	if err != nil {
		return "", fmt.Errorf("failed convert geometry to aoi: %w", err)
	}
	uuid, ok := scene.Data.Metadata[common.UUIDMetadata].(string)
	if !ok {
		return "", fmt.Errorf("failed to get scene uuid: %s", scene.SourceID)
	}

	orderRequest := shared.OrderRequest{
		Kind: "order.data.product",
		Products: []shared.Product{{
			CrsCode:               "urn:ogc:def:crs:EPSG::4326",
			ProductType:           "bundle",
			RadiometricProcessing: "BASIC16",
			Aoi:                   aoi,
			ID:                    uuid,
			ImageFormat:           "image/geotiff"}},
	}

	status, err := o.orderManager.GetStatus(orderRequest)
	switch {
	case err == shared.ErrOrderNotFound:
		accountInformation, err := o.orderManager.GetAccountInformation()
		if err != nil {
			return "", fmt.Errorf("failed to check account information:%w", err)
		}

		price, err := o.orderManager.GetPrice(orderRequest)
		if err != nil {
			return "", fmt.Errorf("failed to check price before start order: %w", err)
		}

		if (accountInformation.Contract.Balance > price.Amount &&
			accountInformation.Contract.Balance-price.Amount > 0) || price.Orderable {
			return "", o.orderManager.Create(orderRequest)
		}
		return "", fmt.Errorf("there are not enough credits to make this order")

	case err != nil:
		return "", fmt.Errorf("failed to get status order: %w", err)
	default:
		// do nothing
	}

	orderStatus := status[uuid]
	switch orderStatus.State {
	case "error", "failed":
		return "", fmt.Errorf("order failed for scene: %s: %v", uuid, orderStatus.Infos)
	case "delivered":
		return orderStatus.DownloadLink, nil
	case "ordered":
		return "", service.MakeTemporary(fmt.Errorf("order for scene %s is still in progress", uuid))
	case "processing":
		return "", service.MakeTemporary(fmt.Errorf("order for scene %s is still in progress", uuid))
	case "rejected":
		return "", fmt.Errorf("order for scene %s is rejected: %v", uuid, orderStatus.Infos)
	default:
		return "", fmt.Errorf("failed to interpret status: %v", orderStatus)
	}
}
