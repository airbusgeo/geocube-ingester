package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/airbusgeo/geocube-ingester/service/log"

	"errors"
)

type tokenManager interface {
	Get() (string, error)
}

type defaultTokenManager struct {
	httpClient            *http.Client
	authenticationAddress string
	apiKey                string
	token                 atomic.Value
	clientID              string
}

func newDefaultTokenManager(ctx context.Context, client *http.Client, authenticationEndpoint string, apikey string, clientID string) (tokenManager, context.CancelFunc) {
	tokenManager := &defaultTokenManager{
		httpClient:            client,
		authenticationAddress: authenticationEndpoint,
		apiKey:                apikey,
		clientID:              clientID,
		token:                 atomic.Value{},
	}

	token, _, err := tokenManager.authenticate(ctx)
	if err != nil {
		log.Logger(ctx).Sugar().Error("failed to authenticate", zap.Any("err", err))
	} else {
		tokenManager.token.Store(token)
	}

	ctx, cncl := context.WithCancel(ctx)

	go func() {
		for {
			var nextRefresh time.Duration
			token, expiration, err := tokenManager.authenticate(ctx)
			if err != nil {
				log.Logger(ctx).Sugar().Error("failed to authenticate", zap.Any("err", err))
				nextRefresh = 30 * time.Second
			} else {
				tokenManager.token.Store(token)
				nextRefresh = 9 * expiration / 10
			}
			log.Logger(ctx).Sugar().Debugf("will refresh token in %s", nextRefresh.String())
			select {
			case <-time.After(nextRefresh):
			case <-ctx.Done():
				return
			}
		}
	}()

	return tokenManager, cncl
}

func (t *defaultTokenManager) Get() (string, error) {
	token, ok := t.token.Load().(string)
	if !ok || token == "" {
		return "", errors.New("failed to get token")
	}
	return token, nil
}

func (t *defaultTokenManager) authenticate(_ context.Context) (string, time.Duration, error) {
	payload := strings.NewReader(t.buildPayload(t.apiKey, t.clientID))

	req, err := http.NewRequest(http.MethodPost, t.authenticationAddress, payload)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r, err := t.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("failed to retrieve jwt: %w", err)
	}

	switch r.StatusCode {
	case http.StatusOK:
	//do nothing
	case http.StatusUnauthorized:
		return "", 0, fmt.Errorf("jmt authentification unauthorized")
	case http.StatusForbidden:
		return "", 0, fmt.Errorf("jmt authentification forbidden")
	default:
		return "", 0, fmt.Errorf("an error occurred")
	}

	jwtResponse := jwtResponse{}
	if err := json.NewDecoder(r.Body).Decode(&jwtResponse); err != nil {
		return "", 0, fmt.Errorf("failed to decode jwt response")
	}

	r.Body.Close()

	if jwtResponse.AccessToken == "" {
		return "", 0, fmt.Errorf("retrieved jwt is empty")
	}
	expiration := time.Duration(jwtResponse.ExpiresIn) * time.Second

	return jwtResponse.AccessToken, expiration, nil
}

type jwtResponse struct {
	AccessToken      string `json:"access_token"`
	ExpiresIn        int    `json:"expires_in"`
	RefreshExpiresIn int    `json:"refresh_expires_in"`
	TokenType        string `json:"token_type"`
	NotBeforePolicy  int    `json:"not-before-policy"`
	SessionState     string `json:"session_state"`
}

func (t *defaultTokenManager) buildPayload(apiKey string, clientID string) string {
	escapedAPIKey := url.QueryEscape(apiKey)
	grantType := "api_key"
	return fmt.Sprintf("apikey=%v&grant_type=%v&client_id=%v", escapedAPIKey, grantType, clientID)
}

type transportJwt struct {
	originalTransport http.RoundTripper
	tokenManager
	blackList []string
}

func (t *transportJwt) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.tokenManager.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	for _, blackListItem := range t.blackList {
		if !strings.EqualFold(req.URL.String(), blackListItem) {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	return t.originalTransport.RoundTrip(req)
}
