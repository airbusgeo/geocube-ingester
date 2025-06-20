package service

import (
	"context"
	"fmt"
	"io"
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

// PageQueryParam provides the information required to request a single page in a Catalog.
type PageQueryParam struct {
	// number of rows to request in a single page
	Limit int
	// page number (0-based)
	Page int
	// First row to select in the page response (0-based)
	FirstRowToSelect int
	// Last row to select in the page response (0-based)
	LastRowToSelect int
}

func QueryGetResult[T any](q *PageQueryParam, slice []T) []T {
	return slice[min(q.FirstRowToSelect, len(slice)):min(q.LastRowToSelect+1, len(slice))]
}

// ComputePagesToQuery translates client paging requirements, provided in terms of page number and page size (limit = the number of rows to return in a single page),
// into catalog paging requirements.
func ComputePagesToQuery(clientPage int, clientLimit int, catalogLimit int) []PageQueryParam {

	// if clientPage or clientLimit have a 0 value, they have not been defined in the request
	if clientLimit == 0 {
		clientLimit = catalogLimit
	}

	// if client's limit is lesser or equal to catalog's limit, the client's page/limit combination can be used as is
	if clientLimit <= catalogLimit {
		return []PageQueryParam{
			{
				Limit:            clientLimit,
				Page:             clientPage,
				FirstRowToSelect: 0,
				LastRowToSelect:  clientLimit - 1,
			},
		}
	}

	// otherwise, client's page/limit must be translated in terms of several catalog pages to be requested, where potentially
	// not all rows have to be selected.
	// translation requires to compute the 0-based position of first and last rows on client's requested page.
	firstRequestedRow := (clientPage * clientLimit)
	lastRequestedRow := firstRequestedRow + clientLimit - 1

	// catalog pages including these rows are computed
	firstCatalogPage := firstRequestedRow / catalogLimit
	lastCatalogPage := lastRequestedRow / catalogLimit

	if firstCatalogPage == lastCatalogPage {
		return []PageQueryParam{
			{
				Limit:            catalogLimit,
				Page:             firstCatalogPage,
				FirstRowToSelect: firstRequestedRow - firstCatalogPage*catalogLimit,
				LastRowToSelect:  lastRequestedRow - firstCatalogPage*catalogLimit,
			},
		}
	} else {
		// first page
		pagesToQuery := []PageQueryParam{
			{
				Limit:            catalogLimit,
				Page:             firstCatalogPage,
				FirstRowToSelect: firstRequestedRow - firstCatalogPage*catalogLimit,
				LastRowToSelect:  catalogLimit - 1,
			},
		}
		// intermediary pages (full pages)
		for page := firstCatalogPage + 1; page < lastCatalogPage; page++ {
			pagesToQuery = append(pagesToQuery, PageQueryParam{
				Limit:            catalogLimit,
				Page:             page,
				FirstRowToSelect: 0,
				LastRowToSelect:  catalogLimit - 1,
			})
		}
		// last page
		pagesToQuery = append(pagesToQuery, PageQueryParam{
			Limit:            catalogLimit,
			Page:             lastCatalogPage,
			FirstRowToSelect: 0,
			LastRowToSelect:  lastRequestedRow - lastCatalogPage*catalogLimit,
		})
		return pagesToQuery
	}
}
