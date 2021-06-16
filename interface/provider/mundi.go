package provider

import (
	"context"
	"fmt"
	"strings"
)

const (
	MundiDownloadProductS1 = "https://mundiwebservices.com/dp/s1-l%s-%s-%04d-q%d/%04d/%02d/%02d/%s/%s/%s.zip"
	MundiDownloadProductS2 = "https://mundiwebservices.com/dp/s2-%s-%04d-q%d/%s/%s/%s/%04d/%02d/%02d/%s.zip"
)

// MundiImageProvider implements ImageProvider for Mundi
type MundiImageProvider struct {
	seeedToken string
}

// NewMundiImageProvider creates a new ImageProvider from Mundi
func NewMundiImageProvider(seeedToken string) *MundiImageProvider {
	return &MundiImageProvider{seeedToken: seeedToken}

}

// Download implements ImageProvider
func (ip *MundiImageProvider) Download(ctx context.Context, sceneName, sceneUUID, localDir string) error {
	var url string
	switch getConstellation(sceneName) {
	case Sentinel1:
		// MMM_BB_TTTR_LFPP_YYYYMMDDTHHMMSS_YYYMMDDTHHMMSS_OOOOOO_DDDDDD_CCCC.SAFE
		sceneDate, err := getDate(sceneName)
		if err != nil {
			return fmt.Errorf("MundiImageProvider.%w", err)
		}
		url = fmt.Sprintf(MundiDownloadProductS1,
			sceneName[12:13], strings.ToLower(sceneName[7:10]), sceneDate.Year(), (sceneDate.Month()+2)/3, sceneDate.Year(), sceneDate.Month(), sceneDate.Day(), sceneName[4:6], sceneName[14:16], sceneName)
	case Sentinel2:
		// MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
		sceneDate, err := getDate(sceneName)
		if err != nil {
			return fmt.Errorf("MundiImageProvider: unable to parse month from scene name %s", sceneName)
		}
		url = fmt.Sprintf(MundiDownloadProductS2,
			sceneName[7:10], sceneDate.Year(), (sceneDate.Month()+2)/3, sceneName[38:40], sceneName[40:41], sceneName[41:43], sceneDate.Year(), sceneDate.Month(), sceneDate.Day(), sceneName)
	default:
		return fmt.Errorf("MundiImageProvider: constellation not supported")
	}

	authorizationToken := "seeedtoken=" + ip.seeedToken
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, "mundi", nil, nil, "Cookie", &authorizationToken); err != nil {
		return fmt.Errorf("MundiImageProvider.%w", err)
	}
	return nil
}
