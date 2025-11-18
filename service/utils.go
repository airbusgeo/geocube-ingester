package service

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"time"

	geocube "github.com/airbusgeo/geocube-client-go/client"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"google.golang.org/grpc/credentials"
)

// NewGeocubeClient connects to the Geocube and returns a client
func NewGeocubeClient(ctx context.Context, geocubeServer, apikey string, tlsConfig *tls.Config) (*geocube.Client, error) {
	if geocubeServer == "" {
		return nil, fmt.Errorf("GeocubeServer undefined")
	}

	var creds credentials.TransportCredentials
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}
	connector := geocube.ClientConnector{Connector: geocube.Connector{Server: geocubeServer, Creds: creds, ApiKey: apikey}}
	gcclient, err := connector.Dial()
	if err != nil {
		return nil, fmt.Errorf("NewGeocubeClient.Dial: %w", err)
	}
	version, err := gcclient.ServerVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewGeocubeClient.Version: %w", err)
	} else {
		log.Logger(ctx).Debug("Connected to Geocube Server " + version)
	}

	return &gcclient, nil
}

// StringSet is a set of strings (all elements are unique)
type StringSet map[string]struct{}

// Push adds the string to the set if not already exists
func (ss StringSet) Push(s string) {
	ss[s] = struct{}{}
}

// Pop removes the string from the set
func (ss StringSet) Pop(s string) {
	delete(ss, s)
}

// Slice returns a slice from the set
func (ss StringSet) Slice() []string {
	sl := make([]string, 0, len(ss))
	for k := range ss {
		sl = append(sl, k)
	}
	return sl
}

// Exists returns true if the string already exists in the Set
func (ss StringSet) Exists(s string) bool {
	_, ok := ss[s]
	return ok
}

// GetBodyRetry: simple GET with N retries in case of temporary errors
func GetBodyRetry(url string, nbRetries int) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("NewRequest: %w", err)
	}
	return GetBodyRetryReq(req, nbRetries)
}

// GetBodyRetry: simple GET with N retries in case of temporary errors
func GetBodyRetryReq(req *http.Request, nbRetries int) ([]byte, error) {
	var e *neturl.Error
	var body []byte
	var err error
	var resp *http.Response

	client := &http.Client{}
	for i := range nbRetries + 1 {
		time.Sleep(((1 << i) - 1) * time.Second) // Exponential backoff, starting at 0
		resp, err = client.Do(req)
		if err != nil {
			if !errors.As(err, &e) || !e.Temporary() {
				return nil, err
			}
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ = io.ReadAll(resp.Body)
			err = fmt.Errorf("%s: %v", resp.Status, body)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return nil, err
			}
			continue
		}
		if body, err = io.ReadAll(resp.Body); err == nil {
			return body, nil
		}
	}
	return nil, err
}
