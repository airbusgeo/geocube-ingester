package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
)

const (
	OndaQueryProduct    = "https://catalogue.onda-dias.eu/dias-catalogue/Products(%s)"
	OndaOrderProduct    = "https://catalogue.onda-dias.eu/dias-catalogue/Products(%s)/Ens.Order"
	OndaDownloadProduct = "https://catalogue.onda-dias.eu/dias-catalogue/Products(%s)/$value"
)
const (
	errMaxRequest   = "Max order requests. Next request: %v"
	maxRequestRetry = 30 * time.Minute // 20 requests per 30 minutes
)

// ONDADiasImageProvider implements ImageProvider for ONDADias
type ONDADiasImageProvider struct {
	user          string
	pword         string
	orderable     bool
	nextOrderTime time.Time
}

// Name implements ImageProvider
func (ip *ONDADiasImageProvider) Name() string {
	return "Onda"
}

// NewONDADiasImageProvider creates a new ImageProvider from ONDADias
func NewONDADiasImageProvider(user, pword string, orderable bool) *ONDADiasImageProvider {
	return &ONDADiasImageProvider{user: user, pword: pword, orderable: orderable}

}

// Download implements ImageProvider
func (ip *ONDADiasImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	sceneUUID := scene.Data.UUID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1, common.Sentinel2:
	default:
		return fmt.Errorf("ONDADiasImageProvider: constellation not supported")
	}

	if ip.orderable {
		// Is product online ?
		offline, err := ip.isOffline(sceneUUID)
		if err != nil {
			return fmt.Errorf("ONDADiasImageProvider.%w", err)
		}
		if offline {
			// Can we order now ?
			if time.Now().Before(ip.nextOrderTime) {
				return service.MakeTemporary(fmt.Errorf("ONDADiasImageProvider: Product is offline. "+errMaxRequest, ip.nextOrderTime))
			}
			status, err := ip.order(sceneUUID)
			if err != nil {
				return fmt.Errorf("ONDADiasImageProvider.%w", err)
			}
			return service.MakeTemporary(fmt.Errorf("ONDADiasImageProvider: Product is offline. %s", status))
		}
	}

	// Download product
	url := fmt.Sprintf(OndaDownloadProduct, sceneUUID)

	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, ip.Name(), &ip.user, &ip.pword, "", nil, false); err != nil {
		return fmt.Errorf("ONDADiasImageProvider.%w", err)
	}
	return nil
}

// Test if a product is offline
func (ip *ONDADiasImageProvider) isOffline(sceneUUID string) (bool, error) {

	// Load scene manifest file
	bodyResponse, err := service.GetBodyRetry(fmt.Sprintf(OndaQueryProduct, sceneUUID), 1)
	if err != nil {
		return false, fmt.Errorf("isOffline.Get: %w", err)
	}

	// Read the "offline" property of the result
	queryResult := struct {
		Offline bool `json:"offline"`
	}{}
	if err := json.Unmarshal(bodyResponse, &queryResult); err != nil {
		return false, fmt.Errorf("ordisOfflineer.Unmarshal(%s): %w", string(bodyResponse), err)
	}

	return queryResult.Offline, nil
}

// Order a product
// Return the status of the order
func (ip *ONDADiasImageProvider) order(sceneUUID string) (string, error) {
	// Order it
	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf(OndaOrderProduct, sceneUUID), nil)
	if err != nil {
		return "", fmt.Errorf("order.NewRequest: %w", err)
	}

	request.SetBasicAuth(ip.user, ip.pword)

	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("order.Do: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		bodyResponse, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("order.ReadAll: %w", err)
		}

		orderResult := struct {
			Status        string `json:"Status"`
			StatusMessage string `json:"StatusMessage"`
			EstimatedTime string `json:"EstimatedTime"`
		}{}
		if err := json.Unmarshal(bodyResponse, &orderResult); err != nil {
			return "", fmt.Errorf("order.Unmarshal(%s): %w", string(bodyResponse), err)
		}

		switch orderResult.Status {
		case "COMPLETED":
			return "", nil
		case "RUNNING":
			return fmt.Sprintf("%s (Estimated Time: %s)", orderResult.StatusMessage, orderResult.EstimatedTime), nil
		}
		// default/FAILED/UNKNOWN:
		return "", fmt.Errorf("order %s: %s", orderResult.Status, orderResult.StatusMessage)

	case 429: // Max requests
		ip.nextOrderTime = time.Now().Add(maxRequestRetry)
		return fmt.Sprintf(errMaxRequest, ip.nextOrderTime), nil
	}
	return "", fmt.Errorf("order: %s", resp.Status)
}
