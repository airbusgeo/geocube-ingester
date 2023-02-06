package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	gstorage "cloud.google.com/go/storage"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube/interface/storage"
	"github.com/airbusgeo/geocube/interface/storage/uri"
	"github.com/mholt/archiver"
)

// Layer of an image
type Layer string

// List of available layers
const (
	Product Layer = "product"

	LayerPreprocessed  Layer = "preprocessed"
	LayerCoregistrated Layer = "coregistred"
	LayerCoregExtract  Layer = "coregextract"
	LayerCoherence     Layer = "coherence"

	LayerBackscatterVV Layer = "sigma0_VV"
	LayerBackscatterVH Layer = "sigma0_VH"
	LayerCoherenceVV   Layer = "coh_VV"
	LayerCoherenceVH   Layer = "coh_VH"
	LayerPanchromatic  Layer = "P"
	LayerMultiSpectral Layer = "MS"
)

// Extension of a layer
type Extension string

// List of supported extension
const (
	ExtensionSAFE      Extension = "SAFE"
	ExtensionZIP       Extension = "zip"
	ExtensionDIMAP     Extension = "dim"
	ExtensionDIMAPData Extension = "data"
	ExtensionDIMAPXml  Extension = "xml"
	ExtensionGTiff     Extension = "tif"
)

// ErrFileNotFound is an error returned by ImportLayer or DeleteLayer
type ErrFileNotFound struct {
	File string
}

func (e ErrFileNotFound) Error() string {
	return fmt.Sprintf("File not found: %s", e.File)
}

func isErrNotFound(err error) bool {
	var epath *os.PathError
	return errors.Is(err, gstorage.ErrObjectNotExist) ||
		(errors.As(err, &epath) && os.IsNotExist(epath))
}

// LayerFileName returns the name of the file given the tile, the layer and the extension
func LayerFileName(tile common.Tile, layer Layer, ext Extension) string {
	if layer == Product {
		return fmt.Sprintf("%s.%s", tile.Scene.SourceID, ext)
	}
	return fmt.Sprintf("%s_%s_%s.%s", tile.Scene.Data.Date.Format("20060102"), tile.SourceID, layer, ext)
}

// Storage is a service to store and retrieve file from storage
type Storage interface {
	// SaveLayer persists the layer into a storage and returns the uri
	SaveLayer(ctx context.Context, tile common.Tile, layer Layer, ext Extension, localdir string) (string, error)
	// ImportLayer imports the layer from the storage to the given localdir
	// Raise ErrFileNotFound
	ImportLayer(ctx context.Context, tile common.Tile, layer Layer, ext Extension, localdir string) error
	// DeleteLayer delete the layer from the storage
	// Raise ErrFileNotFound
	DeleteLayer(ctx context.Context, tile common.Tile, layer Layer, ext Extension) error
}

// StorageStrategy implements Storage using geocube.Strategy
type StorageStrategy struct {
	storage storage.Strategy
	uri     uri.DefaultUri
}

// NewStorageStrategy creates a new StorageStrategy
func NewStorageStrategy(ctx context.Context, storageURI string) (*StorageStrategy, error) {
	uri, err := uri.ParseUri(storageURI)
	if err != nil {
		return nil, fmt.Errorf("NewStorageStrategy.ParseURI: %w", err)
	}

	storageClient, err := uri.NewStorageStrategy(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewStorageStrategy: %w", err)
	}

	return &StorageStrategy{storage: storageClient, uri: uri}, nil
}

// SaveLayer implements Storage
func (ss *StorageStrategy) SaveLayer(ctx context.Context, tile common.Tile, layer Layer, ext Extension, localdir string) (string, error) {
	src := path.Join(localdir, LayerFileName(tile, layer, ext))

	if ext == ExtensionDIMAP {
		ext = ExtensionZIP
		src = withExt(src, ext)
		if err := archiver.NewZip().Archive([]string{withExt(src, ExtensionDIMAP), withExt(src, ExtensionDIMAPData)}, src); err != nil {
			return "", fmt.Errorf("SaveLayer.Archive: %w", err)
		}
		defer os.Remove(src)
	} else if ext == ExtensionSAFE {
		ext = ExtensionZIP
		src = withExt(src, ext)
		if err := archiver.NewZip().Archive([]string{withExt(src, ExtensionSAFE)}, src); err != nil {
			return "", fmt.Errorf("SaveLayer.Archive: %w", err)
		}
		defer os.Remove(src)
	}

	f, err := os.Open(src)
	if err != nil {
		return "", fmt.Errorf("SaveLayer.Open: %w", err)
	}
	defer f.Close()

	dst := ss.getPath(tile, LayerFileName(tile, layer, ext))
	if err := ss.storage.UploadFile(ctx, dst, f); err != nil {
		return "", fmt.Errorf("SaveLayer.UploadFromFile to %s: %w", dst, err)
	}

	return dst, nil
}

// ImportLayer implements Storage
func (ss *StorageStrategy) ImportLayer(ctx context.Context, tile common.Tile, layer Layer, ext Extension, localdir string) error {
	if ext == ExtensionDIMAP || ext == ExtensionSAFE {
		ext = ExtensionZIP
	}

	layerFileName := LayerFileName(tile, layer, ext)
	src := ss.getPath(tile, layerFileName)
	dst := path.Join(localdir, layerFileName)
	if err := ss.storage.DownloadToFile(ctx, src, dst); err != nil {
		if isErrNotFound(err) {
			return ErrFileNotFound{src}
		}
		return fmt.Errorf("ImportLayer.DownloadToFile from %s: %w", src, err)
	}

	if ext == ExtensionZIP {
		zip := archiver.Zip{OverwriteExisting: true}
		if err := zip.Unarchive(dst, path.Dir(dst)); err != nil {
			return fmt.Errorf("ImportLayer.Unarchive: %w", err)
		}
		os.Remove(dst)
	}

	return nil
}

// DeleteLayer implements Storage
func (ss *StorageStrategy) DeleteLayer(ctx context.Context, tile common.Tile, layer Layer, ext Extension) error {
	if ext == ExtensionDIMAP || ext == ExtensionSAFE {
		ext = ExtensionZIP
	}

	file := ss.getPath(tile, LayerFileName(tile, layer, ext))
	if err := ss.storage.Delete(ctx, file); err != nil {
		if isErrNotFound(err) {
			return ErrFileNotFound{file}
		}
		return fmt.Errorf("DeleteLayer.Delete: %w", err)
	}

	return nil
}

// getPath returns the local path of the layer of the tile
func (ss *StorageStrategy) getPath(tile common.Tile, filename string) string {
	uri := ss.uri.String()
	if !strings.HasSuffix(uri, "/") {
		uri += "/"
	}
	return uri + path.Join(tile.Scene.AOI, tile.Scene.SourceID, "tiles", tile.SourceID, filename)
}

func withExt(filepath string, ext Extension) string {
	return filepath[:len(filepath)-len(getExt(filepath))] + string(ext)
}

func getExt(filepath string) Extension {
	return Extension(path.Ext(filepath)[1:])
}
