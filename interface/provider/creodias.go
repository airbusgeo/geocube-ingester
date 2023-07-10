package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
)

const (
	CreodiasToken  = "https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token"
	CreodiasSearch = "https://finder.creodias.eu/resto/api/collections/%s/search.json?productIdentifier=%%25%s%%25"
)

// CreoDiasImageProvider implements ImageProvider for CreoDias
type CreoDiasImageProvider struct {
	user   string
	pword  string
	token  string
	expire time.Time
}

// Name implements ImageProvider
func (ip *CreoDiasImageProvider) Name() string {
	return "CreoDias"
}

// LoadCreoDiasToken loads the download token
func (ip *CreoDiasImageProvider) LoadCreoDiasToken() error {
	// Ask for token
	resp, err := http.PostForm(CreodiasToken,
		url.Values{
			"client_id":  {"CLOUDFERRO_PUBLIC"},
			"username":   {ip.user},
			"password":   {ip.pword},
			"grant_type": {"password"}})
	if err != nil {
		return fmt.Errorf("CreoDiasToken.PostForm: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("CreoDiasToken.ReadAll: %w", err)
	}
	defer resp.Body.Close()

	token := struct {
		AccessToken string `json:"access_token"`
		Expire      int    `json:"expires_in"`
	}{}

	if err := json.Unmarshal(body, &token); err != nil {
		return fmt.Errorf("CreoDiasToken.Unmarshall: %w", err)
	}
	if token.AccessToken == "" {
		return fmt.Errorf("CreoDiasToken : token not found in %s", string(body))
	}

	ip.token = token.AccessToken
	ip.expire = time.Now().Add(time.Duration(token.Expire) * time.Second)
	return nil
}

// NewCreoDiasImageProvider creates a new ImageProvider from CreoDias
func NewCreoDiasImageProvider(user, pword string) *CreoDiasImageProvider {
	return &CreoDiasImageProvider{user: user, pword: pword, expire: time.Now()}
}

// Download implements ImageProvider
func (ip *CreoDiasImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	var searchUrl string
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1:
		searchUrl = fmt.Sprintf(CreodiasSearch, "Sentinel1", sceneName)
	case common.Sentinel2:
		searchUrl = fmt.Sprintf(CreodiasSearch, "Sentinel2", sceneName)
	default:
		return fmt.Errorf("CreoDiasImageProvider: constellation not supported")
	}

	// Retrieve the download URL
	url, err := getDownloadURL(searchUrl)
	if err != nil {
		if errors.Is(err, ErrProductNotFound{}) {
			err = ErrProductNotFound{sceneName}
		}
		return fmt.Errorf("CreoDiasImageProvider.%w", err)
	}

	// Load token
	if time.Now().After(ip.expire) || ip.token == "" {
		if err := ip.LoadCreoDiasToken(); err != nil {
			return fmt.Errorf("CreoDiasImageProvider.Download.%w", err)
		}
	}

	url += "?token=" + ip.token
	if err := downloadZipWithAuth(ctx, url, localDir, sceneName, ip.Name(), &ip.user, &ip.pword, "", nil, false); err != nil {
		return fmt.Errorf("CreoDiasImageProvider.%w", err)
	}
	return nil
}
