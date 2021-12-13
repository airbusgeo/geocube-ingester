package provider

import (
	"context"
)

// ImageProvider is the interface of an image download service
type ImageProvider interface {
	// Download an image to the given localDir
	// sceneName is for example S1A_IW_SLC__1SDV_20190103T170131_20190103T170159_025316_02CD10_519D
	// localDir is the directory where the image will be stored
	Download(ctx context.Context, sceneName, sceneUUID, localDir string) error

	// Name of the provider
	Name() string
}
