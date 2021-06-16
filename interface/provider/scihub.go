package provider

import (
	"context"
	"fmt"
)

const scihubDownloadProduct = "https://scihub.copernicus.eu/dhus/odata/v1/Products('%s')/$value"

// ScihubImageProvider implements ImageProvider for Scihub
type ScihubImageProvider struct {
	user  string
	pword string
}

// NewScihubImageProvider creates a new ImageProvider from Scihub
func NewScihubImageProvider(user, pword string) *ScihubImageProvider {
	return &ScihubImageProvider{user: user, pword: pword}

}

// Download implements ImageProvider
func (ip *ScihubImageProvider) Download(ctx context.Context, sceneName, sceneUUID, localDir string) error {
	switch getConstellation(sceneName) {
	case Sentinel1, Sentinel2:
	default:
		return fmt.Errorf("ScihubImageProvider: constellation not supported")
	}

	url := fmt.Sprintf(scihubDownloadProduct, sceneUUID)
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, "scihub", &ip.user, &ip.pword, "", nil); err != nil {
		return fmt.Errorf("ScihubImageProvider.%w", err)
	}
	return nil
}
