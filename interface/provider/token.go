package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/airbusgeo/geocube/interface/storage/uri"
)

type CachedValue struct {
	Value         string
	Expire        time.Time
	CacheURI      string
	CacheFilename string
}

// Valid returns true if the value exists and is not expired
func (ct *CachedValue) Valid() bool {
	return ct.Value != "" && time.Now().Before(ct.Expire)
}

// ValidWithCache returns true if the value is valid. If not, try to load the value from the cache
func (ct *CachedValue) ValidWithCache(ctx context.Context) (bool, error) {
	if !ct.Valid() {
		if err := ct.Load(ctx); err != nil {
			return false, fmt.Errorf("validWithCache.%w", err)
		}
	}
	return ct.Valid(), nil
}

func (ct *CachedValue) getCachedValueUri() (uri.DefaultUri, error) {
	valueUrl, err := url.JoinPath(ct.CacheURI, ct.CacheFilename)
	if err != nil {
		return uri.DefaultUri{}, fmt.Errorf("getCachedValueUri.join: %w", err)
	}

	cachedValueURI, err := uri.ParseUri(valueUrl)
	if err != nil {
		return uri.DefaultUri{}, fmt.Errorf("getCachedValueUri.parse: %w", err)
	}
	return cachedValueURI, nil
}

// Load loads value from cache using URI Download
// Returns nil if CacheURI not defined
func (ct *CachedValue) Load(ctx context.Context) error {
	if ct.CacheURI == "" {
		return nil
	}
	// Construct cache URI for value file
	cachedValueURI, err := ct.getCachedValueUri()
	if err != nil {
		return fmt.Errorf("Load.%w", err)
	}

	// Download cached value data
	data, err := cachedValueURI.Download(ctx)
	if err != nil {
		return fmt.Errorf("Load.download: %w", err)
	}

	// Parse cached value data
	var cachedData struct {
		Value  string    `json:"value"`
		Expire time.Time `json:"expire"`
	}

	if err := json.Unmarshal(data, &cachedData); err != nil {
		return fmt.Errorf("Load.unmarshal: %w", err)
	}

	// If there is no change, return
	if ct.Value == cachedData.Value {
		return nil
	}

	// Check if value is still valid
	if time.Now().Before(cachedData.Expire) {
		ct.Value = cachedData.Value
		ct.Expire = cachedData.Expire
	}

	return nil
}

// Save saves value to cache using URI Upload
func (ct *CachedValue) Save(ctx context.Context, value string, expire time.Time) error {
	ct.Value = value
	ct.Expire = expire
	if ct.CacheURI == "" {
		return nil
	}

	// Construct cache URI for value file
	cachedValueURI, err := ct.getCachedValueUri()
	if err != nil {
		return fmt.Errorf("Save.%w", err)
	}

	// Prepare cache data
	cacheData := struct {
		Value  string    `json:"value"`
		Expire time.Time `json:"expire"`
	}{
		Value:  ct.Value,
		Expire: ct.Expire,
	}

	data, err := json.Marshal(cacheData)
	if err != nil {
		return fmt.Errorf("Save.marshal: %w", err)
	}

	// Upload to cache
	if err := cachedValueURI.Upload(ctx, data); err != nil {
		return fmt.Errorf("Save.upload: %w", err)
	}

	return nil
}
