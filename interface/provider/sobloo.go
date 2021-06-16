package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
func (ip *SoblooImageProvider) Download(ctx context.Context, sceneName, sceneUUID, localDir string) error {
	switch getConstellation(sceneName) {
	case Sentinel1, Sentinel2:
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
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, "sobloo", nil, nil, "Authorization", &authorization_key); err != nil {
		return fmt.Errorf("SoblooImageProvider.%w", err)
	}
	return nil
}
