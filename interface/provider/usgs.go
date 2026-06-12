// Package provider implements interfaces for downloading satellite imagery from various providers.
// This package includes support for USGS M2M API to fetch Landsat data.
// Based https://m2m.cr.usgs.gov/api/docs/json/
// https://m2m.cr.usgs.gov/api/docs/reference/#download-options
// An access request is necessary : https://ers.cr.usgs.gov/profile/access
package provider

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
)

var (
	AUTHENTICATION_MAX_TRIES      = 20
	AUTHENTICATION_MIN_TIME_SLEEP = 3
	AUTHENTICATION_MAX_TIME_SLEEP = 12
)

const (
	M2M_API_URL                   = "https://m2m.cr.usgs.gov/api/api/json/stable/"
	M2M_LOGIN_TOKEN_ENDPOINT      = "login-token"
	M2M_DOWNLOAD_OPTIONS_ENDPOINT = "download-options"
	M2M_DOWNLOAD_REQUEST_ENDPOINT = "download-request"
	M2M_DATASET                   = "landsat_ot_c2_l1"
	M2M_AUTH_APIKEY_HEADER_KEY    = "X-Auth-Token"

	M2M_CACHED_APIKEY_FILENAME = "m2m_apikey.cache"
)

type UsgsM2MImageProvider struct {
	username   string
	token      string
	apiKey     CachedValue
	httpClient *http.Client
}

type m2mErrorResponse struct {
	ErrorCode    interface{} `json:"errorCode"`
	ErrorMessage interface{} `json:"errorMessage"`
}

type M2MAuthResponse struct {
	m2mErrorResponse
	Data string `json:"data"`
}

type M2MSceneSearchResponse struct {
	m2mErrorResponse
	Data struct {
		Results []struct {
			EntityId string `json:"entityId"`
			// Other scene metadata fields
		} `json:"results"`
	} `json:"data"`
}

type m2mDownloadOptionsRequest struct {
	EntityIds   string `json:"entityIds"`
	DatasetName string `json:"datasetName"`
}

type m2mDownloadOptionsResponse struct {
	m2mErrorResponse
	Data []struct {
		ProductId   string `json:"id"`
		ProductName string `json:"productName"`
		Available   bool   `json:"available"`
	} `json:"data"`
}

type m2mDownloadsData struct {
	EntityId  string `json:"entityId"`
	ProductId string `json:"productId"`
}

type m2mDownloadParams struct {
	DownloadsData       []m2mDownloadsData `json:"downloads"`
	DownloadApplication string             `json:"downloadApplication"`
}

type m2mDownloadUrl struct {
	Url string `json:"url"`
}

type m2mDownloadData struct {
	AvailableDownloads []m2mDownloadUrl `json:"availableDownloads"`
}

type m2mDownloadResponse struct {
	m2mErrorResponse
	DownloadData m2mDownloadData `json:"data"`
}

// Name implements ImageProvider
func (ip *UsgsM2MImageProvider) Name() string {
	return "UsgsM2M"
}

// NewUsgsM2MImageProvider creates a new ImageProvider for USGS M2M API
// cacheURI can be empty if no caching is desired
func NewUsgsM2MImageProvider(username, token string, cacheURI string) *UsgsM2MImageProvider {
	return &UsgsM2MImageProvider{
		username: username,
		token:    token,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		apiKey: CachedValue{
			CacheURI:      cacheURI,
			CacheFilename: M2M_CACHED_APIKEY_FILENAME,
		},
	}
}

// Download implements ImageProvider
func (ip *UsgsM2MImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {

	sceneName := scene.SourceID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Landsat89:
	default:
		return fmt.Errorf("UsgsM2MImageProvider: constellation not supported")
	}

	// 1. Authenticate if needed
	if err := ip.ensureAuthenticated(ctx); err != nil {
		return fmt.Errorf("UsgsM2MImageProvider.%w", err)
	}

	// 2. Get entityId
	entityId, ok := scene.Data.Metadata[common.EntityIdMetadata].(string)
	if !ok || entityId == "" {
		return fmt.Errorf("UsgsM2MImageProvider: failed to get %s from Metadata", common.EntityIdMetadata)
	}

	// 3. Get download options
	productId, err := ip.getDownloadOptions(ctx, entityId)
	if err != nil {
		return fmt.Errorf("UsgsM2MImageProvider.%w", err)
	}

	// 4. Request download URL
	downloadURL, err := ip.getDownloadURL(ctx, entityId, productId)
	if err != nil {
		return fmt.Errorf("UsgsM2MImageProvider.%w", err)
	}

	// 5. Download and unarchive
	if err := downloadArchiveWithAuth(ctx, downloadURL, localDir, service.ExtensionTAR, sceneName, ip.Name(), nil, nil, M2M_AUTH_APIKEY_HEADER_KEY, &ip.apiKey.Value, true); err != nil {
		return fmt.Errorf("UsgsM2MImageProvider.%w", err)
	}

	return nil
}

// ensureAuthenticated ensures we have a valid API key
func (ip *UsgsM2MImageProvider) ensureAuthenticated(ctx context.Context) error {
	nbTries := 0

	for {
		// Check if API key is still valid. ReadCache error is ignored
		if ok, err := ip.apiKey.ValidWithCache(ctx); ok {
			return nil
		} else if err != nil {
			log.Logger(ctx).Sugar().Debugf("%v", err)
		}

		// authentication
		if err := ip.authenticate(ctx); err == nil {
			return nil
		} else if !service.Temporary(err) {
			return fmt.Errorf("ensureAuthenticated: %w", err)
		} else {
			log.Logger(ctx).Sugar().Debugf("## [%d/%d] temporary error: %v", nbTries+1, AUTHENTICATION_MAX_TRIES, err)
		}

		// code below is only active if a temporary error occured: if maximum number of tries
		// is not reached, we loop after waiting for a number of seconds
		if nbTries >= AUTHENTICATION_MAX_TRIES {
			return fmt.Errorf("ensureAuthenticated loop: give up after %d tries", AUTHENTICATION_MAX_TRIES)
		}
		nbTries++
		// sleep random time between min and max values
		time_sleep := time.Duration(rand.Float32()*float32(AUTHENTICATION_MAX_TIME_SLEEP-AUTHENTICATION_MIN_TIME_SLEEP)+float32(AUTHENTICATION_MIN_TIME_SLEEP)) * time.Second
		time.Sleep(time_sleep)
	}
}

// authenticate authenticates with M2M API and caches the api-key
func (ip *UsgsM2MImageProvider) authenticate(ctx context.Context) error {
	authURL := M2M_API_URL + M2M_LOGIN_TOKEN_ENDPOINT

	authRequest := map[string]string{
		"username": ip.username,
		"token":    ip.token,
	}

	reqBody, err := json.Marshal(authRequest)
	if err != nil {
		return fmt.Errorf("authenticate.marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", authURL, strings.NewReader(string(reqBody)))
	if err != nil {
		return fmt.Errorf("authenticate.request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ip.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("authenticate.do: %w", err)
	}
	defer resp.Body.Close()

	// Transforms the HTTP status code into an error, a temporary error or nil (if 2xx)
	httpErr := httpStatusCode2Error(resp.StatusCode)

	var authResp M2MAuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		body, _ := io.ReadAll(resp.Body)
		if httpErr != nil {
			return fmt.Errorf("authenticate.decode: %v [%w]: %s", err, httpErr, body)
		}
		return fmt.Errorf("authenticate.decode: %w: %s", err, body)
	}
	if authResp.ErrorCode != nil {
		return fmt.Errorf("authenticate: [ErrCode: %v] %s [%w]", authResp.ErrorCode, authResp.ErrorMessage, httpErr)
	}
	if httpErr != nil {
		return fmt.Errorf("authenticate: %w", httpErr)
	}

	if err := ip.apiKey.Save(ctx, authResp.Data, time.Now().Add(2*time.Hour)); err != nil {
		// Log warning but don't fail the authentication
		fmt.Printf("Warning: Failed to cache M2M apikey: %v\n", err)
	}

	return nil
}

// m2mRequest executes an authenticated M2M API request with auto-login.
// Retries once on RATE_LIMIT (the global M2M lock should prevent most rate limits).
func (ip *UsgsM2MImageProvider) m2mRequest(ctx context.Context, method, endpoint string, body []byte, entityId string) ([]byte, error) {
	u, _ := url.Parse(M2M_API_URL + endpoint)

	nb_tries := 0

	for {
		if err := ip.ensureAuthenticated(ctx); err != nil {
			return nil, fmt.Errorf("m2mRequest[%s][%s].%w", endpoint, entityId, err)
		}

		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewBuffer(body))
		if err != nil {
			return nil, fmt.Errorf("m2mRequest.NewRequest[%s][%s]: %w", endpoint, entityId, err)
		}
		req.Header.Set(M2M_AUTH_APIKEY_HEADER_KEY, ip.apiKey.Value)

		resp, err := ip.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("m2mRequest.Do[%s][%s]: %w", endpoint, entityId, err)
		}
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("m2mRequest.ReadAll[%s][%s]: %w", endpoint, entityId, err)
		}

		// Transforms the HTTP status code into an error, a temporary error or nil (if 2xx)
		httpErr := httpStatusCode2Error(resp.StatusCode)

		if httpErr == nil {
			return respBody, nil
		}

		// HTTP status code indicates an error: parse error response
		errResp := m2mErrorResponse{}
		err = json.Unmarshal(respBody, &errResp)
		if err != nil {
			return nil, fmt.Errorf("m2mRequest.Unmarshal[%s][%s]: %w [%w]", endpoint, entityId, err, httpErr)
		}
		errorCodeAndMessage := fmt.Sprintf("ErrorCode=%s, ErrorMessage=%s", errResp.ErrorCode, errResp.ErrorMessage)
		if !service.Temporary(httpErr) {
			return nil, fmt.Errorf("m2mRequest[%s][%s]: %s [%w]", endpoint, entityId, errorCodeAndMessage, httpErr)
		} else {
			log.Logger(ctx).Sugar().Debugf("## m2mRequest[%s][%s][%d/%d] temporary error: %v [%v]", endpoint, entityId, nb_tries+1, AUTHENTICATION_MAX_TRIES, errorCodeAndMessage, httpErr)
		}

		// code below is only active if a temporary error occured: if maximum number of tries
		// is not reached, we loop after waiting for a number of seconds
		if nb_tries >= AUTHENTICATION_MAX_TRIES {
			return nil, fmt.Errorf("m2mRequest[%s][%s] loop: give up after %d tries", endpoint, entityId, AUTHENTICATION_MAX_TRIES)
		} else {
			nb_tries++
			// sleep random time between min and max values
			time_sleep := time.Duration(rand.Float32()*float32(AUTHENTICATION_MAX_TIME_SLEEP-AUTHENTICATION_MIN_TIME_SLEEP)+float32(AUTHENTICATION_MIN_TIME_SLEEP)) * time.Second
			time.Sleep(time_sleep)
		}
	}
}

// getDownloadOptions gets available download options for a scene
func (ip *UsgsM2MImageProvider) getDownloadOptions(ctx context.Context, entityId string) (string, error) {
	downloadOptionsRequest := m2mDownloadOptionsRequest{EntityIds: entityId, DatasetName: M2M_DATASET}
	reqBody, err := json.Marshal(downloadOptionsRequest)
	if err != nil {
		return "", fmt.Errorf("getDownloadOptions.marshal: %w", err)
	}

	respBody, err := ip.m2mRequest(ctx, "POST", M2M_DOWNLOAD_OPTIONS_ENDPOINT, reqBody, entityId)
	if err != nil {
		return "", fmt.Errorf("getDownloadOptions.%w", err)
	}

	var downloadOptionsResp m2mDownloadOptionsResponse
	if err := json.Unmarshal(respBody, &downloadOptionsResp); err != nil {
		return "", fmt.Errorf("getDownloadOptions.unmarshal: %w", err)
	}

	if downloadOptionsResp.ErrorCode != nil {
		return "", fmt.Errorf("getDownloadOptions: %s", downloadOptionsResp.ErrorMessage)
	}

	// Find the first available product
	for _, result := range downloadOptionsResp.Data {
		if result.ProductName == "Landsat Collection 2 Level-1 Product Bundle" && result.Available {
			return result.ProductId, nil
		}
	}

	return "", ErrProductNotFound{entityId}
}

// getDownloadOptions gets available download options for a scene
func (ip *UsgsM2MImageProvider) getDownloadURL(ctx context.Context, entityID, productID string) (string, error) {
	dlParams := m2mDownloadParams{
		DownloadApplication: "EE",
		DownloadsData: []m2mDownloadsData{
			{EntityId: entityID, ProductId: productID},
		},
	}
	data, err := json.Marshal(dlParams)
	if err != nil {
		return "", fmt.Errorf("getDownloadURL.marshal: %w", err)
	}

	respBody, err := ip.m2mRequest(ctx, "POST", M2M_DOWNLOAD_REQUEST_ENDPOINT, data, entityID)
	if err != nil {
		return "", fmt.Errorf("getDownloadURL.%w", err)
	}

	dlResp := m2mDownloadResponse{}
	if err := json.Unmarshal(respBody, &dlResp); err != nil {
		return "", fmt.Errorf("getDownloadURL.unmarshal: %w", err)
	}
	if dlResp.ErrorCode != nil {
		return "", fmt.Errorf("getDownloadURL.ErrorCode: %v: %v", dlResp.ErrorCode, dlResp.ErrorMessage)
	}
	if len(dlResp.DownloadData.AvailableDownloads) == 0 {
		return "", fmt.Errorf("getDownloadURL: no download URL available for entity %s", entityID)
	}

	return dlResp.DownloadData.AvailableDownloads[0].Url, nil
}
