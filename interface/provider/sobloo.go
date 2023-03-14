package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/airbusgeo/geocube-ingester/common"
)

// SoblooImageProvider implements ImageProvider for Sobloo
type SoblooImageProvider struct {
	apikey string
}

const (
	SoblooHost            = "https://sobloo.eu"
	SoblooSearch          = "/api/v1/services/search?"
	SoblooDownloadOrder   = "/api/v1/services/order/products/orders/%d/deliveries/%s/download"
	SoblooDownloadProduct = "/api/v1/services/download/%s"
	SoblooGetOrder        = "/api/v1/services/order/products/orders/%d"
)

// NewSoblooImageProvider creates a new ImageProvider from Sobloo
func NewSoblooImageProvider(apikey string) *SoblooImageProvider {
	return &SoblooImageProvider{apikey: apikey}
}

func (ip *SoblooImageProvider) Name() string {
	return "Sobloo"
}

func (ip *SoblooImageProvider) NewRequestWithAuth(ctx context.Context, method, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("NewRequestWithAuth: %w", err)
	}
	req.Header.Add("Authorization", "Apikey "+ip.apikey)
	return req, nil
}

func (ip *SoblooImageProvider) getInternalId(ctx context.Context, sceneName string) (string, error) {
	req, err := ip.NewRequestWithAuth(ctx, "GET", fmt.Sprintf(SoblooHost+SoblooSearch+"f=identification.externalId:eq:%s&include=identification", sceneName))
	if err != nil {
		return "", fmt.Errorf("getInternalId.%w", err)
	}

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("getInternalId.Do: %w", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("getInternalId.ReadAll: %w", err)
	}
	defer resp.Body.Close()

	jsonResults := struct {
		Hits []struct {
			Data struct {
				ID string `json:"uid"`
			} `json:"data"`
		} `json:"hits"`
	}{}

	if err := json.Unmarshal(body, &jsonResults); err != nil || len(jsonResults.Hits) == 0 {
		return "", fmt.Errorf("getInternalId.Unmarshal: %w", err)
	}
	return jsonResults.Hits[0].Data.ID, nil
}

// Download implements ImageProvider
func (ip *SoblooImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1, common.Sentinel2:
	default:
		return fmt.Errorf("SoblooImageProvider: constellation not supported")
	}

	// Get internal id
	iid, err := ip.getInternalId(ctx, sceneName)
	if err != nil {
		return fmt.Errorf("SoblooImageProvider.%w", err)
	}

	url := fmt.Sprintf(SoblooHost+SoblooDownloadProduct, iid)
	authorization_key := "Apikey " + ip.apikey
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, ip.Name(), nil, nil, "Authorization", &authorization_key, false); err != nil {
		return fmt.Errorf("SoblooImageProvider.%w", err)
	}
	return nil
}
