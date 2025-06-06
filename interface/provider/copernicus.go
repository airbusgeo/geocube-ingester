package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
)

const copernicusDownloadProduct = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products(%s)/$value"
const copernicusAuth = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

// CopernicusImageProvider implements ImageProvider for Copernicus
type CopernicusImageProvider struct {
	user   string
	pword  string
	token  string
	expire time.Time
}

// Name implements ImageProvider
func (ip *CopernicusImageProvider) Name() string {
	return "Copernicus"
}

// LoadCopernicusToken loads the download token
func (ip *CopernicusImageProvider) LoadCopernicusToken() error {
	// Ask for token
	resp, err := http.PostForm(copernicusAuth,
		url.Values{
			"client_id":  {"cdse-public"},
			"username":   {ip.user},
			"password":   {ip.pword},
			"grant_type": {"password"}})
	if err != nil {
		return fmt.Errorf("CopernicusToken.PostForm: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("CopernicusToken.ReadAll: %w", err)
	}
	defer resp.Body.Close()

	token := struct {
		AccessToken string `json:"access_token"`
		Expire      int    `json:"expires_in"`
	}{}

	if err := json.Unmarshal(body, &token); err != nil {
		return fmt.Errorf("CopernicusToken.Unmarshall: %w", err)
	}
	if token.AccessToken == "" {
		return fmt.Errorf("CopernicusToken : token not found in %s", string(body))
	}

	ip.token = token.AccessToken
	ip.expire = time.Now().Add(time.Duration(token.Expire) * time.Second)
	return nil
}

// NewCopernicusImageProvider creates a new ImageProvider from Copernicus
func NewCopernicusImageProvider(user, pword string) *CopernicusImageProvider {
	return &CopernicusImageProvider{user: user, pword: pword}

}

// Download implements ImageProvider
func (ip *CopernicusImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1, common.Sentinel2:
	default:
		return fmt.Errorf("CopernicusImageProvider: constellation not supported")
	}
	sceneUUID, ok := scene.Data.Metadata[common.UUIDMetadata]
	if !ok {
		return fmt.Errorf("CopernicusImageProvider: uuid not found in metadata")
	}

	url := fmt.Sprintf(copernicusDownloadProduct, sceneUUID)

	// Load token
	if time.Now().After(ip.expire) || ip.token == "" {
		if err := ip.LoadCopernicusToken(); err != nil {
			return fmt.Errorf("CopernicusImageProvider.Download.%w", err)
		}
	}

	token := "Bearer " + ip.token
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, ip.Name(), nil, nil, "Authorization", &token, true); err != nil {
		return fmt.Errorf("CopernicusImageProvider.%w", err)
	}
	return nil
}
