package service

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
)

func HTTPGetWithAuth(ctx context.Context, url, authName, authPswd, authToken string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTPGet: %w", err)
	}
	resp, err := doWithAuth(req, authName, authPswd, authToken)
	if err != nil {
		return nil, fmt.Errorf("HTTPGet: %w", err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func HTTPPostWithAuth(ctx context.Context, url string, body io.Reader, authName, authPswd, authToken string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("HTTPGet: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	return doWithAuth(req, authName, authPswd, authToken)
}

func doWithAuth(req *http.Request, authName, authPswd, authToken string) (*http.Response, error) {
	if authName != "" {
		req.SetBasicAuth(authName, authPswd)
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	client := http.Client{}
	return client.Do(req)
}

// PageLimitRows returns the page limit and the optimal number of rows for a PageRows request
func PageLimitRows(page, limit, defautRows int) (int, int) {
	if limit == 0 {
		return math.MaxInt64, defautRows
	}
	if limit%defautRows != 0 {
		defautRows = limit
	}
	return page + limit/defautRows, defautRows
}

// IndexLimitRows returns the first index and the last index for an IndexRows request
func IndexLimitRows(page, limit int) (int, int) {
	if limit == 0 {
		return 0, math.MaxInt64
	}
	return page * limit, (page + 1) * limit
}
