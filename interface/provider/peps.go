package provider

import (
	"context"
	"errors"
	"fmt"
)

const (
	PEPSSearch = "https://peps.cnes.fr/resto/api/collections/search.json?"
)

// PEPSDiasImageProvider implements ImageProvider for PEPSDias
type PEPSDiasImageProvider struct {
	user  string
	pword string
}

// NewPEPSDiasImageProvider creates a new ImageProvider from PEPSDias
func NewPEPSDiasImageProvider(user, pword string) *PEPSDiasImageProvider {
	return &PEPSDiasImageProvider{user: user, pword: pword}
}

// Download implements ImageProvider
func (ip *PEPSDiasImageProvider) Download(ctx context.Context, sceneName, sceneUUID, localDir string) error {
	switch getConstellation(sceneName) {
	case Sentinel1, Sentinel2:
	default:
		return fmt.Errorf("PEPSDiasImageProvider: constellation not supported")
	}

	// Get download url
	url, err := getDownloadURL(PEPSSearch + "q=" + sceneName)
	if err != nil {
		if errors.Is(err, ErrProductNotFound{}) {
			err = ErrProductNotFound{sceneName}
		}
		return fmt.Errorf("PEPSDiasImageProvider.%w", err)
	}

	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, "peps", &ip.user, &ip.pword, "", nil); err != nil {
		return fmt.Errorf("PEPSDiasImageProvider.%w", err)
	}
	return nil
}
