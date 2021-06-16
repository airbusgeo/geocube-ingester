package provider

import (
	"context"
	"fmt"
)

const (
	OndaDownloadProduct = "https://catalogue.onda-dias.eu/dias-catalogue/Products(%s)/$value"
)

// ONDADiasImageProvider implements ImageProvider for ONDADias
type ONDADiasImageProvider struct {
	user  string
	pword string
}

// NewONDADiasImageProvider creates a new ImageProvider from ONDADias
func NewONDADiasImageProvider(user, pword string) *ONDADiasImageProvider {
	return &ONDADiasImageProvider{user: user, pword: pword}

}

// Download implements ImageProvider
func (ip *ONDADiasImageProvider) Download(ctx context.Context, sceneName, sceneUUID, localDir string) error {
	switch getConstellation(sceneName) {
	case Sentinel1, Sentinel2:
	default:
		return fmt.Errorf("ONDADiasImageProvider: constellation not supported")
	}

	url := fmt.Sprintf(OndaDownloadProduct, sceneUUID)

	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, "onda", &ip.user, &ip.pword, "", nil); err != nil {
		return fmt.Errorf("ONDADiasImageProvider.%w", err)
	}
	return nil
}
