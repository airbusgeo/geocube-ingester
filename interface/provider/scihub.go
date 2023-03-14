package provider

import (
	"context"
	"fmt"

	"github.com/airbusgeo/geocube-ingester/common"
)

const scihubDownloadProduct = "https://scihub.copernicus.eu/dhus/odata/v1/Products('%s')/$value"

// ScihubImageProvider implements ImageProvider for Scihub
type ScihubImageProvider struct {
	user  string
	pword string
}

// Name implements ImageProvider
func (ip *ScihubImageProvider) Name() string {
	return "Scihub"
}

// NewScihubImageProvider creates a new ImageProvider from Scihub
func NewScihubImageProvider(user, pword string) *ScihubImageProvider {
	return &ScihubImageProvider{user: user, pword: pword}

}

// Download implements ImageProvider
func (ip *ScihubImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	sceneUUID := scene.Data.UUID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1, common.Sentinel2:
	default:
		return fmt.Errorf("ScihubImageProvider: constellation not supported")
	}

	url := fmt.Sprintf(scihubDownloadProduct, sceneUUID)
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, ip.Name(), &ip.user, &ip.pword, "", nil, false); err != nil {
		return fmt.Errorf("ScihubImageProvider.%w", err)
	}
	return nil
}
