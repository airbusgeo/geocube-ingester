package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
)

type orderManagerImpl struct {
	orderEndpoint string
	apikey        string
	client        *http.Client
}

type OrderManager interface {
	Create(orderRequest OrderRequest) error
	GetStatus(orderRequest OrderRequest) (map[string]OrderStatus, error)
	GetPrice(orderRequest OrderRequest) (OrderPrice, error)
	GetAccountInformation() (AccountInformation, error)
}

func NewOrderManager(ctx context.Context, orderEndpoint, authenticationEndpoint, apikey string) *orderManagerImpl {
	client := &http.Client{}

	client.Transport = &transportJwt{
		originalTransport: http.DefaultTransport,
		blackList:         []string{authenticationEndpoint},
		tokenManager: newDefaultTokenManager(
			ctx,
			client,
			authenticationEndpoint,
			apikey,
			"IDP"),
	}

	return &orderManagerImpl{
		client:        client,
		apikey:        apikey,
		orderEndpoint: orderEndpoint,
	}
}

func (m *orderManagerImpl) Create(orderRequest OrderRequest) error {
	bodyRequest, err := json.Marshal(&orderRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal body request: %w", err)
	}

	url := m.orderEndpoint + "/api/v1/orders"
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(bodyRequest))
	if err != nil {
		return fmt.Errorf("failed to create new request: %w", err)
	}

	resp, err := m.client.Do(request)
	if err != nil {
		return fmt.Errorf("failed to execute http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create new order")
	}
	return nil
}

func (m *orderManagerImpl) GetStatus(orderRequest OrderRequest) (map[string]OrderStatus, error) {
	itemPerPage := 100
	startPage := 1

	nbPage := 0
	deliveries := make(map[string]OrderStatus)
	for {
		requestURL := m.orderEndpoint + "/api/v1/orders" + fmt.Sprintf("?kind=order.data.gb.product&itemsPerPage=%d&page=%d", itemPerPage, startPage)

		request, err := http.NewRequest(http.MethodGet, requestURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create new request: %w", err)
		}

		resp, err := m.client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("failed to execute http request: %w", err)
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read body response")
		}

		var orderStatusResponse orderStatusResponse
		if err := json.Unmarshal(b, &orderStatusResponse); err != nil {
			return nil, fmt.Errorf("failed to unmarshal order status response: %w", err)
		}

		resp.Body.Close()

		if orderStatusResponse.TotalResults == 0 {
			return nil, ErrOrderNotFound
		}

		for _, item := range orderStatusResponse.Items {
			for _, delivery := range item.Deliveries {
				if contains(delivery.Datas, orderRequest.Products) {
					deliveries[delivery.Datas.ID] = OrderStatus{
						State:        delivery.Status,
						DownloadLink: delivery.Links.Download.Href,
						Price:        delivery.Price,
						DataID:       delivery.Datas.ID,
						OrderID:      item.ID,
						Infos: map[string]string{
							"contractID": item.ContractID,
							"message":    delivery.Message,
							"errCode":    delivery.ErrorCode,
						},
					}
				}
			}
		}

		nbPage = orderStatusResponse.TotalResults/itemPerPage + 1
		if startPage < nbPage {
			startPage++
		} else {
			break
		}
	}

	if len(deliveries) == 0 {
		return nil, ErrOrderNotFound
	}

	return deliveries, nil
}

func (m *orderManagerImpl) GetPrice(orderRequest OrderRequest) (OrderPrice, error) {
	payload, err := json.Marshal(&orderRequest)
	if err != nil {
		return OrderPrice{}, fmt.Errorf("failed to marshal payload to check %v: %w", orderRequest, err)
	}

	url := m.orderEndpoint + "/api/v1/prices"
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return OrderPrice{}, fmt.Errorf("failed to create http request: %w", err)
	}

	resp, err := m.client.Do(request)
	if err != nil {
		return OrderPrice{}, fmt.Errorf("failed to execute http request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return OrderPrice{}, fmt.Errorf("failed to check order price")
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return OrderPrice{}, fmt.Errorf("failed to read body response")
	}

	var orderPriceResponse orderPriceResponse
	if err := json.Unmarshal(b, &orderPriceResponse); err != nil {
		return OrderPrice{}, fmt.Errorf("failed to unmarshal order status response: %w", err)
	}

	orderPrice := orderPriceResponse.Price
	orderPrice.Orderable = strings.EqualFold(orderPriceResponse.Orderable, "ok")
	orderPrice.Deliveries = orderPriceResponse.Deliveries
	return orderPrice, nil
}

func (m *orderManagerImpl) GetAccountInformation() (AccountInformation, error) {
	url := m.orderEndpoint + "/api/v1/me"
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return AccountInformation{}, fmt.Errorf("failed to create http request: %w", err)
	}

	resp, err := m.client.Do(request)
	if err != nil {
		return AccountInformation{}, fmt.Errorf("failed to execute http request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return AccountInformation{}, fmt.Errorf("failed to check account information")
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return AccountInformation{}, fmt.Errorf("failed to read body response")
	}

	var aInformation AccountInformation
	if err := json.Unmarshal(b, &aInformation); err != nil {
		return AccountInformation{}, fmt.Errorf("failed to unmarshal order status response: %w", err)
	}

	return aInformation, nil
}

func contains(item datas, list []Product) bool {
	for _, product := range list {
		if strings.EqualFold(item.ID, product.ID) &&
			item.Aoi.Equal(product.Aoi) &&
			strings.EqualFold(item.CrsCode, product.CrsCode) &&
			strings.EqualFold(item.ProductType, product.ProductType) &&
			strings.EqualFold(item.RadiometricProcessing, product.RadiometricProcessing) {
			return true
		}
	}
	return false
}

type OrderStatus struct {
	State        string
	DownloadLink string
	Price        int
	OrderID      string
	DataID       string
	Infos        map[string]string
}

type OrderRequest struct {
	Kind     string    `json:"kind"`
	Products []Product `json:"products"`
}

type Product struct {
	CrsCode               string `json:"crsCode"`
	ProductType           string `json:"productType"`
	RadiometricProcessing string `json:"radiometricProcessing"`
	Aoi                   Aoi    `json:"aoi"`
	ID                    string `json:"id"`
	ImageFormat           string `json:"imageFormat"`
}

type Aoi struct {
	Coordinates [][][2]float64 `json:"coordinates"`
	Type        string         `json:"type"`
}

func (a Aoi) Equal(otherAOI Aoi) bool {
	if strings.EqualFold(a.Type, otherAOI.Type) {
		if len(a.Coordinates) == len(otherAOI.Coordinates) {
			for i := 0; i < len(a.Coordinates); i++ {
				ring1 := a.Coordinates[i]
				ring2 := otherAOI.Coordinates[i]
				if len(ring1) == len(ring2) {
					for j := 0; j < len(ring1); j++ {
						if ring1[j][0] == ring2[j][0] && ring1[j][1] == ring2[j][1] {
							continue
						}
						if ring1[j][0] == ring2[len(ring1)-1-j][0] && ring1[j][1] == ring2[len(ring1)-1-j][1] {
							continue
						}
						return false

					}
				} else {
					return false
				}
			}
		} else {
			return false
		}
	}
	return true
}

type orderStatusResponse struct {
	StartIndex        int  `json:"startIndex"`
	TotalResults      int  `json:"totalResults"`
	CountLimitReached bool `json:"countLimitReached"`
	ItemsPerPage      int  `json:"itemsPerPage"`
	Items             []struct {
		ID                string `json:"id"`
		Kind              string `json:"kind"`
		Status            string `json:"status"`
		ContractID        string `json:"contractId"`
		IsAmountEstimated string `json:"isAmountEstimated"`
		Price             int    `json:"price"`
		Amount            int    `json:"amount"`
		AmountUnit        string `json:"amountUnit"`
		EstimatedAmount   int    `json:"estimatedAmount"`
		Deliveries        []struct {
			Links struct {
				Download struct {
					Href string `json:"href"`
				} `json:"download"`
			} `json:"_links"`
			ID                string    `json:"id"`
			Kind              string    `json:"kind"`
			Status            string    `json:"status"`
			IsAmountEstimated string    `json:"isAmountEstimated"`
			Price             int       `json:"price"`
			Amount            int       `json:"amount"`
			EstimatedAmount   int       `json:"estimatedAmount"`
			AmountUnit        string    `json:"amountUnit"`
			Area              string    `json:"area"`
			Unit              string    `json:"unit"`
			Datas             datas     `json:"datas"`
			UpdatedAt         time.Time `json:"updatedAt"`
			DeliveredAt       time.Time `json:"deliveredAt"`
			ErrorCode         string    `json:"errorCode,omitempty"`
			Message           string    `json:"message,omitempty"`
		} `json:"deliveries"`
	} `json:"items"`
}

type datas struct {
	ProductType           string `json:"productType"`
	RadiometricProcessing string `json:"radiometricProcessing"`
	ImageFormat           string `json:"imageFormat"`
	CrsCode               string `json:"crsCode"`
	ID                    string `json:"id"`
	Aoi                   Aoi    `json:"aoi"`
	SourceID              string `json:"sourceId"`
	Sensor                string `json:"sensor"`
	SegmentFootprint      struct {
		Coordinates [][][]float64 `json:"coordinates"`
		Type        string        `json:"type"`
	} `json:"segmentFootprint"`
	ProcessingLevel string  `json:"processingLevel"`
	AreaKm2         float64 `json:"areaKm2"`
}

type orderPriceResponse struct {
	Payload struct {
		Kind     string    `json:"kind"`
		Products []Product `json:"products"`
	} `json:"payload"`
	Deliveries  []delivery `json:"deliveries"`
	Price       OrderPrice `json:"price"`
	Feasibility string     `json:"feasibility"`
	Orderable   string     `json:"orderable"`
}

type delivery struct {
	Datas struct {
		Aoi struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		} `json:"aoi"`
		ProductType           string `json:"productType"`
		RadiometricProcessing string `json:"radiometricProcessing"`
		ImageFormat           string `json:"imageFormat"`
		CrsCode               string `json:"crsCode"`
		ID                    string `json:"id"`
		SourceID              string `json:"sourceId"`
	} `json:"datas"`
	ID                string  `json:"id"`
	Amount            int     `json:"amount"`
	AmountUnit        string  `json:"amountUnit"`
	IsAmountEstimated string  `json:"isAmountEstimated"`
	AreaKm2           float64 `json:"areaKm2"`
}

type OrderPrice struct {
	Credits           int        `json:"credits"`
	Amount            int        `json:"amount"`
	IsAmountEstimated string     `json:"isAmountEstimated"`
	AreaKm2           float64    `json:"areaKm2"`
	AmountUnit        string     `json:"amountUnit"`
	Orderable         bool       `json:"-"`
	Deliveries        []delivery `json:"-"`
}

type AccountInformation struct {
	Links struct {
		Self struct {
			Href string `json:"href"`
		} `json:"self"`
		Licenses struct {
			Href string `json:"href"`
		} `json:"licenses"`
	} `json:"_links"`
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	Roles     []string  `json:"roles"`
	Contract  struct {
		Links struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			Orders struct {
				Href string `json:"href"`
			} `json:"orders"`
			Payments struct {
				Href string `json:"href"`
			} `json:"payments"`
			Subscriptions struct {
				Href string `json:"href"`
			} `json:"subscriptions"`
			Deliveries struct {
				Href string `json:"href"`
			} `json:"deliveries"`
			Reports struct {
				Href string `json:"href"`
			} `json:"reports"`
		} `json:"_links"`
		ID          string    `json:"id"`
		Status      string    `json:"status"`
		CreatedAt   time.Time `json:"createdAt"`
		Offers      []string  `json:"offers"`
		AmountUnit  string    `json:"amountUnit"`
		Kind        string    `json:"kind"`
		Name        string    `json:"name"`
		Balance     int       `json:"balance"`
		WorkspaceID string    `json:"workspaceId"`
	} `json:"contract"`
}

const (
	ErrOrderNotFound = orderErr("order does not exist")
)

type orderErr string

func (o orderErr) Error() string {
	return string(o)
}

func GetAoiFromGeometry(scene common.Scene) (Aoi, error) {
	aoiGeometry, ok := scene.Data.Metadata["geometry"]
	if !ok {
		return Aoi{}, fmt.Errorf("failed to get geometry metadata")
	}

	b, err := json.Marshal(aoiGeometry)
	if err != nil {
		return Aoi{}, fmt.Errorf("failed to marshal aoiGeometry: %w", err)
	}

	var a Aoi
	if err := json.Unmarshal(b, &a); err != nil {
		return Aoi{}, fmt.Errorf("failed to unmarshal into aoi: %w", err)
	}
	return a, nil
}
