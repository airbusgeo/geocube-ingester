package provider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
)

// LocalImageProvider implements ImageProvider for local storage
type LocalImageProvider struct {
	path string
}

// Name implements ImageProvider
func (ip *LocalImageProvider) Name() string {
	return "FileSystem (" + ip.path + ")"
}

// NewLocalImageProvider creates a new ImageProvider from local storage
func NewLocalImageProvider(path string) *LocalImageProvider {
	return &LocalImageProvider{path: path}
}

// Download implements ImageProvider
func (ip *LocalImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	// Retrieve date of the scene from name
	sceneName := scene.SourceID
	var srcZip string
	date, err := common.GetDateFromProductId(sceneName)
	var verr *time.ParseError
	if err != nil && !errors.As(err, &verr) {
		return fmt.Errorf("LocalImageProvider: %w", err)
	} else if err != nil {
		srcZip = path.Join(ip.path, sceneName+".zip")
	} else {
		// Create the list of subfolders
		folders := strings.Split(date.Format("2006-01-02"), "-")
		srcZip = path.Join(ip.path, folders[0], folders[1], folders[2], sceneName+".zip")
	}

	// Unarchive file
	if _, err := os.Stat(srcZip); err != nil {
		if os.IsNotExist(err) {
			return ErrProductNotFound{srcZip}
		}
		return fmt.Errorf("LocalImageProvider: %w", err)

	}
	/*localZip := sceneFilePath(localDir, sceneName, service.ExtensionZIP)
	if err := fileCopy(srcZip, localZip); err != nil {
		return fmt.Errorf("LocalImageProvider.Download: %w", err)
	}
	defer os.Remove(localZip)*/
	if err := unarchive(ctx, srcZip, localDir); err != nil {
		return fmt.Errorf("LocalImageProvider.Unarchive: %w", err)
	}
	return nil
}

// fileCopy copies a single file from src to dst
/*func fileCopy(src, dst string) error {
	input, err := io.ReadFile(src)
	if err != nil {
		return fmt.Errorf("fileCopy.ReadFile: %w", err)
	}

	_ = os.MkdirAll(path.Dir(dst), 0700)
	if err = io.WriteFile(dst, input, 0644); err != nil {
		return fmt.Errorf("fileCopy.WriteFile: %w", err)
	}
	return nil
}
*/
