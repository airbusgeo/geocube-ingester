package service

import (
	"fmt"
	"reflect"
	"testing"
)

func checkPageQueryParams(t *testing.T, clientPage int, clientLimit int, catalogLimit int, pageQueryParams []PageQueryParam, pageQueryParamsRef []PageQueryParam) {
	if reflect.DeepEqual(pageQueryParams, pageQueryParamsRef) == false {
		fmt.Printf("---- PageQueryParams ----- clientPage=%d clientLimit=%d catalogLimit=%d\n", clientPage, clientLimit, catalogLimit)
		for _, pageQueryParam := range pageQueryParams {
			fmt.Printf("  Limit: %d Page %d firstRowToSelect: %d lastRowToSelect: %d\n", pageQueryParam.Limit, pageQueryParam.Page, pageQueryParam.FirstRowToSelect, pageQueryParam.LastRowToSelect)
		}
		t.Errorf("ComputePagesToQuery(%d, %d, %d)", clientPage, clientLimit, catalogLimit)
	}
}

func TestComputePagesToQuery(t *testing.T) {

	clientPage := 1
	clientLimit := 10
	catalogLimit := 10
	pageQueryParams := ComputePagesToQuery(clientPage, clientLimit, catalogLimit)
	pageQueryParamsRef := []PageQueryParam{
		{Limit: 10, Page: 1, FirstRowToSelect: 0, LastRowToSelect: 9},
	}
	checkPageQueryParams(t, clientPage, clientLimit, catalogLimit, pageQueryParams, pageQueryParamsRef)

	clientPage = 2
	clientLimit = 3
	catalogLimit = 2
	pageQueryParams = ComputePagesToQuery(clientPage, clientLimit, catalogLimit)
	pageQueryParamsRef = []PageQueryParam{
		{Limit: 2, Page: 3, FirstRowToSelect: 0, LastRowToSelect: 1},
		{Limit: 2, Page: 4, FirstRowToSelect: 0, LastRowToSelect: 0},
	}
	checkPageQueryParams(t, clientPage, clientLimit, catalogLimit, pageQueryParams, pageQueryParamsRef)

	clientPage = 1
	clientLimit = 3
	catalogLimit = 2
	pageQueryParams = ComputePagesToQuery(clientPage, clientLimit, catalogLimit)
	pageQueryParamsRef = []PageQueryParam{
		{Limit: 2, Page: 1, FirstRowToSelect: 1, LastRowToSelect: 1},
		{Limit: 2, Page: 2, FirstRowToSelect: 0, LastRowToSelect: 1},
	}
	checkPageQueryParams(t, clientPage, clientLimit, catalogLimit, pageQueryParams, pageQueryParamsRef)

	clientPage = 1
	clientLimit = 7
	catalogLimit = 2
	pageQueryParams = ComputePagesToQuery(clientPage, clientLimit, catalogLimit)
	pageQueryParamsRef = []PageQueryParam{
		{Limit: 2, Page: 3, FirstRowToSelect: 1, LastRowToSelect: 1},
		{Limit: 2, Page: 4, FirstRowToSelect: 0, LastRowToSelect: 1},
		{Limit: 2, Page: 5, FirstRowToSelect: 0, LastRowToSelect: 1},
		{Limit: 2, Page: 6, FirstRowToSelect: 0, LastRowToSelect: 1},
	}
	checkPageQueryParams(t, clientPage, clientLimit, catalogLimit, pageQueryParams, pageQueryParamsRef)

	//t.Errorf("not implemented")
}
