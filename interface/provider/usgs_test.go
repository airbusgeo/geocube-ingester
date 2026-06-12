package provider

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
)

var user = os.Getenv("USGS_USERNAME")
var token = os.Getenv("USGS_TOKEN")
var sourceId = "LC09_L1TP_196021_20220104_20230502_02_T1"
var entityId = "LC91960212022004LGN02"

func TestUsgsM2MImageProvider_ensureAuthenticated(t *testing.T) {
	provider := NewUsgsM2MImageProvider(user, token, "")

	if err := provider.ensureAuthenticated(context.Background()); err != nil {
		t.Errorf("Expected valid credentials. got: %v", err)
	}
}

func TestUsgsM2MImageProvider_TokenCaching(t *testing.T) {
	provider := NewUsgsM2MImageProvider("testuser", "testtoken", "/tmp/test_usgs_cache")

	if err := provider.apiKey.Save(context.Background(), "test", time.Now().Add(-time.Hour)); err != nil {
		t.Errorf("Expected no error. got: %v", err)
	}

	provider.apiKey.Value = ""
	if err := provider.apiKey.Load(context.Background()); err != nil {
		t.Errorf("Expected no error. got: %v", err)
	}
	if provider.apiKey.Value != "" {
		t.Errorf("Expected no apikey (expired). got: %s", provider.apiKey.Value)
	}

	if err := provider.apiKey.Save(context.Background(), "test", time.Now().Add(time.Hour)); err != nil {
		t.Errorf("Expected no error. got: %v", err)
	}

	provider.apiKey.Value = ""
	if err := provider.apiKey.Load(context.Background()); err != nil {
		t.Errorf("Expected no error. got: %v", err)
	}
	if provider.apiKey.Value != "test" {
		t.Errorf("Expected apikey (not expired). got: %s", provider.apiKey.Value)
	}
}

func TestUsgsM2MImageProvider_GetDownloadURL(t *testing.T) {
	dname, err := os.MkdirTemp("", "usgs_test")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	defer os.RemoveAll(dname)

	provider := NewUsgsM2MImageProvider(user, token, path.Join(dname, "usgs_cache"))

	productId, err := provider.getDownloadOptions(context.Background(), entityId)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if productId == "" {
		t.Errorf("Expected productId, got: empty")
	}
	downloadUrl, err := provider.getDownloadURL(context.Background(), entityId, productId)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if downloadUrl == "" {
		t.Errorf("Expected downloadUrl, got: empty")
	}
}

func TestUsgsM2MImageProvider_Download(t *testing.T) {
	dname, err := os.MkdirTemp("", "usgs_test")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	defer os.RemoveAll(dname)

	provider := NewUsgsM2MImageProvider(user, token, path.Join(dname, "usgs_cache"))

	scene := common.Scene{
		SourceID: sourceId,
		Data: common.SceneAttrs{
			Metadata: map[string]interface{}{
				common.EntityIdMetadata: entityId,
			},
		},
	}

	// Test the Download function
	if err = provider.Download(context.Background(), scene, path.Join(dname, "download_dir")); err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

}
func TestUsgsM2MImageProvider_TokenExpiration(t *testing.T) {
	provider := NewUsgsM2MImageProvider("testuser", "testtoken", "")

	// Set a token that will be expired
	provider.apiKey.Save(context.Background(), "test-key", time.Now().Add(-3*time.Hour))

	// Should try to re-authenticate
	err := provider.ensureAuthenticated(context.Background())
	if err == nil {
		t.Error("Expected error with invalid credentials")
	}
}
