package provider

import (
	"context"
	"fmt"

	"github.com/airbusgeo/geocube-ingester/common"
)

const (
	ASFDownloadProductSLC = "https://datapool.asf.alaska.edu/SLC/S{MISSION_VERSION}/{SCENE}.zip"
	ASFDownloadProductGRD = "https://datapool.asf.alaska.edu/GRD-HD/S{MISSION_VERSION}/{SCENE}.zip"
)

// ASFImageProvider implements ImageProvider for Alaska Satellite Facility
type ASFImageProvider struct {
	token string
}

// Name implements ImageProvider
func (ip *ASFImageProvider) Name() string {
	return "ASF"
}

// NewASFImageProvider creates a new ImageProvider from ASF
func NewASFImageProvider(token string) *ASFImageProvider {
	return &ASFImageProvider{token: token}
}

// Download implements ImageProvider
func (ip *ASFImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1:
	default:
		return fmt.Errorf("ASFImageProvider: constellation not supported")
	}

	info, err := common.Info(sceneName)
	if err != nil {
		return fmt.Errorf("ASFImageProvider.%w", err)
	}
	var url string
	switch info["PRODUCT_TYPE"] {
	case "SLC":
		url = ASFDownloadProductSLC
	case "GRD":
		url = ASFDownloadProductGRD
	default:
		return fmt.Errorf("ASFImageProvider: not supported product type: %s", info["PRODUCT_TYPE"])
	}
	url = common.FormatBrackets(url, info)

	token := "Bearer " + ip.token
	if err = downloadZipWithAuth(ctx, url, localDir, sceneName, ip.Name(), nil, nil, "Authorization", &token, true); err != nil {
		return fmt.Errorf("ASFImageProvider.%w", err)
	}
	return nil
}
