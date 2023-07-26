package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/xlzd/gotp"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
)

const (
	CreodiasToken         = "https://identity.cloudferro.com/auth/realms/Creodias-new/protocol/openid-connect/token"
	CreodiasSearch        = "https://datahub.creodias.eu/odata/v1/Products?$filter=Name%%20eq%%20'%s.SAFE'"
	CreodiasDownload      = "https://datahub.creodias.eu/odata/v1/Products(%s)/$value"
	CreodiasTokenFilename = "creodias_token.cache"
)

// CreoDiasImageProvider implements ImageProvider for CreoDias
type CreoDiasImageProvider struct {
	user       string
	pword      string
	totpSecret string
	token      CachedValue
}

// Name implements ImageProvider
func (ip *CreoDiasImageProvider) Name() string {
	return "CreoDias"
}

// LoadCreoDiasToken loads the download token
// Handles Time-based One-Time Password (TOTP) generation if a TOTP secret is provided.
// totpExpirationMargin: A time.Duration specifying the margin before TOTP expiration to wait for the next TOTP.
// Return Values
// time.Duration: The time taken to complete the token loading process.
// error: An error if the token loading process fails.
func (ip *CreoDiasImageProvider) LoadCreoDiasToken(ctx context.Context, totpExpirationMargin time.Duration) (time.Duration, error) {
	data := url.Values{
		"client_id":  {"CLOUDFERRO_PUBLIC"},
		"username":   {ip.user},
		"password":   {ip.pword},
		"grant_type": {"password"}}

	// Generate TOTP code
	if ip.totpSecret != "" {
		totpGen := gotp.NewDefaultTOTP(ip.totpSecret)
		totp, expirationTime := totpGen.NowWithExpiration()
		expirationDelay := time.Until(time.Unix(expirationTime, 0))
		// If expirationTime is too early, wait until the next TOTP
		if expirationDelay < totpExpirationMargin {
			time.Sleep(expirationDelay + 1)
			return ip.LoadCreoDiasToken(ctx, totpExpirationMargin)
		}
		data["totp"] = []string{totp}
	}
	requestStart := time.Now()

	resp, err := http.PostForm(CreodiasToken, data)
	if err != nil {
		return time.Since(requestStart), fmt.Errorf("CreoDiasToken.PostForm: %w", err)
	}
	defer resp.Body.Close()

	// Transforms the HTTP status code into an error, a temporary error or nil (if 2xx)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return time.Since(requestStart), fmt.Errorf("CreoDiasToken.ReadAll: %w", err)
	}
	if resp.StatusCode == 401 {
		return time.Since(requestStart), service.MakeTemporary(fmt.Errorf("LoadCreoDiasToken: http %d: %s", resp.StatusCode, body))
	}
	if httpErr := httpStatusCode2Error(resp.StatusCode); httpErr != nil {
		return time.Since(requestStart), fmt.Errorf("LoadCreoDiasToken: %w: %s", httpErr, body)
	}

	token := struct {
		AccessToken string `json:"access_token"`
		Expire      int    `json:"expires_in"`
	}{}

	if err := json.Unmarshal(body, &token); err != nil {
		return time.Since(requestStart), fmt.Errorf("CreoDiasToken.Unmarshall: %w", err)
	}
	if token.AccessToken == "" {
		return time.Since(requestStart), fmt.Errorf("CreoDiasToken : token not found in %s (%v: %v=>%v)", string(body), time.Now(), time.Now().Unix(), data["totp"])
	}

	ip.token.Save(ctx, token.AccessToken, time.Now().Add(time.Duration(token.Expire)*time.Second))
	return time.Since(requestStart), nil
}

// ensureAuthenticated ensures we have a valid API key
func (ip *CreoDiasImageProvider) ensureAuthenticated(ctx context.Context) error {
	nbTries := 0
	totpExpirationMargin := 2 * time.Second

	for {
		// Check if token is still valid. ReadCache error is ignored
		if ok, err := ip.token.ValidWithCache(ctx); ok {
			return nil
		} else if err != nil {
			log.Logger(ctx).Sugar().Debugf("%v", err)
		}

		// authentication
		requestDuration, err := ip.LoadCreoDiasToken(ctx, totpExpirationMargin)
		if err == nil {
			return nil
		} else if !service.Temporary(err) {
			return fmt.Errorf("ensureAuthenticated.%w", err)
		}
		log.Printf("## [%d/%d] temporary error: %v", nbTries+1, AUTHENTICATION_MAX_TRIES, err)

		// code below is only active if a temporary error occured: if maximum number of tries
		// is not reached, we loop after waiting for a number of seconds
		if nbTries >= AUTHENTICATION_MAX_TRIES {
			return fmt.Errorf("ensureAuthenticated loop: give up after %d tries", AUTHENTICATION_MAX_TRIES)
		}
		if requestDuration > totpExpirationMargin && requestDuration < (30-1) {
			totpExpirationMargin = requestDuration
		}
		nbTries++
		// sleep random time between min and max values
		time_sleep := time.Duration(rand.Float32()*float32(AUTHENTICATION_MAX_TIME_SLEEP-AUTHENTICATION_MIN_TIME_SLEEP)+float32(AUTHENTICATION_MIN_TIME_SLEEP)) * time.Second
		time.Sleep(time_sleep)
	}
}

func (ip *CreoDiasImageProvider) getSceneId(sceneName string) (string, error) {
	searchUrl := fmt.Sprintf(CreodiasSearch, sceneName)
	resp, err := http.Get(searchUrl)
	if err != nil {
		return "", fmt.Errorf("getSceneId.Get: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("getSceneId.ReadAll: %w", err)
	}
	defer resp.Body.Close()

	jsonURL := struct {
		Values []struct {
			Id string `json:"Id"`
		} `json:"value"`
	}{}

	if err := json.Unmarshal(body, &jsonURL); err != nil || len(jsonURL.Values) == 0 {
		if err == nil {
			return "", ErrProductNotFound{}
		}
		return "", fmt.Errorf("getSceneId.Unmarshal [%s]: %w", body, err)
	}

	return jsonURL.Values[0].Id, nil
}

// NewCreoDiasImageProvider creates a new ImageProvider from CreoDias
// totpSecret is the 32-character base32 TOTP secret for 2FA authentication
func NewCreoDiasImageProvider(user, pword, totpSecret, cacheURI string) *CreoDiasImageProvider {
	return &CreoDiasImageProvider{user: user, pword: pword, totpSecret: totpSecret,
		token: CachedValue{CacheURI: cacheURI, CacheFilename: CreodiasTokenFilename}}
}

// Download implements ImageProvider
func (ip *CreoDiasImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Sentinel1, common.Sentinel2:
	default:
		return fmt.Errorf("CreoDiasImageProvider: constellation not supported")
	}

	// Retrieve the scene UUID and the download URI
	sceneUUID, ok := scene.Data.Metadata[common.UUIDMetadata]
	if !ok {
		var err error
		sceneUUID, err = ip.getSceneId(sceneName)
		if err != nil {
			return fmt.Errorf("CreoDiasImageProvider.%w", err)
		}
	}
	url := fmt.Sprintf(CreodiasDownload, sceneUUID)

	// Load token
	if err := ip.ensureAuthenticated(ctx); err != nil {
		return fmt.Errorf("CreoDiasImageProvider.Download.%w", err)
	}

	url += "?token=" + ip.token.Value
	if err := downloadArchiveWithAuth(ctx, url, localDir, service.ExtensionZIP, sceneName, ip.Name(), &ip.user, &ip.pword, "", nil, false); err != nil {
		return fmt.Errorf("CreoDiasImageProvider.%w", err)
	}
	return nil
}
