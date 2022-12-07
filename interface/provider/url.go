package provider

import (
	"context"
	"fmt"
	"os"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube/interface/storage/uri"
)

// URLImageProvider implements ImageProvider for direct download link
type URLImageProvider struct {
}

// Name implements ImageProvider
func (ip *URLImageProvider) Name() string {
	return "URL"
}

// NewURLImageProvider creates a new ImageProvider for direct download link
func NewURLImageProvider() *URLImageProvider {
	return &URLImageProvider{}
}

// Download implements ImageProvider
func (ip *URLImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID

	if scene.Data.Metadata == nil {
		return fmt.Errorf("URLImageProvider: unable to retrieve download Link: scene metadata is empty")
	}

	downloadLink, ok := scene.Data.Metadata[common.DownloadLinkMetadata].(string)
	if !ok || downloadLink == "" {
		return fmt.Errorf("URLImageProvider: downloadLink not found in scene.metadata")
	}

	uri, err := uri.ParseUri(downloadLink)
	if err != nil {
		return fmt.Errorf("URLImageProvider: %w", err)
	}

	ext := service.GetExt(downloadLink)

	var localFile string
	switch uri.Protocol() {
	case "file", "":
		localFile = downloadLink
	default:
		localFile = sceneFilePath(localDir, sceneName, ext)
		if err = uri.DownloadToFile(ctx, localFile); err != nil {
			return fmt.Errorf("URLImageProvider: %w", err)
		}
		if ext == service.ExtensionZIP {
			defer os.Remove(localFile)
		}
	}
	if ext == service.ExtensionZIP {
		if err := unarchive(ctx, localFile, localDir); err != nil {
			return service.MakeTemporary(fmt.Errorf("URLImageProvider.Unarchive: %w", err))
		}
	}
	return nil
}
