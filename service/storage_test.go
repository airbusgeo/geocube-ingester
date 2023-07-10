package service

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
)

func initLocalDirs() (string, string, string, error) {
	localdir, err := os.MkdirTemp("", "local")
	if err != nil {
		return "", "", "", err
	}
	distdir, err := os.MkdirTemp("", "dist")
	if err != nil {
		return "", "", "", err
	}
	localdir2, err := os.MkdirTemp("", "local2")
	return localdir, distdir, localdir2, err
}

func createFiles(dir, name string) {
	os.WriteFile(path.Join(dir, name+"dim"), []byte("test"), 0644)
	os.Mkdir(path.Join(dir, name+"data"), 0755)
	os.WriteFile(path.Join(dir, name+"data", "data"), []byte("test"), 0644)
	os.WriteFile(path.Join(dir, name+"tif"), []byte("test"), 0644)
}

func TestLocalStorage(t *testing.T) {
	ctx := context.Background()

	localdir, distdir, localdir2, err := initLocalDirs()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(localdir)
	defer os.RemoveAll(localdir2)
	defer os.RemoveAll(distdir)

	tile := common.Tile{
		SourceID: "A44_IW1_1561",
		Scene: common.Scene{
			SourceID: "S1A",
			AOI:      "test",
			Data: common.SceneAttrs{
				Date: time.Date(2019, 1, 3, 0, 0, 0, 0, time.Local),
			},
		},
	}
	layer := LayerPreprocessed
	createFiles(localdir, LayerFileName(tile, layer, ""))

	service, err := NewStorageStrategy(ctx, distdir)
	if err != nil {
		t.Error(err)
	}

	testStorage(t, ctx, localdir, localdir2, tile, layer, service)
}

func TestGStorage(t *testing.T) {
	ctx := context.Background()

	localdir, destdir, localdir2, err := initLocalDirs()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(localdir)
	defer os.RemoveAll(destdir)
	defer os.RemoveAll(localdir2)

	tile := common.Tile{
		SourceID: "A44_IW1_1561",
		Scene: common.Scene{
			SourceID: "S1A",
			AOI:      "test",
			Data: common.SceneAttrs{
				Date: time.Date(2019, 1, 3, 0, 0, 0, 0, time.Local),
			},
		},
	}
	layer := LayerPreprocessed
	createFiles(localdir, LayerFileName(tile, layer, ""))

	storage, err := NewStorageStrategy(ctx, "gs://geocube-sar-data")
	if err != nil {
		t.Error(err)
	}

	testStorage(t, ctx, localdir, localdir2, tile, layer, storage)
}

func testStorage(t *testing.T, ctx context.Context, localdir, localdir2 string, tile common.Tile, layer Layer, storage Storage) {
	// Save tile.dim
	if _, err := storage.SaveLayer(ctx, tile, layer, ExtensionDIMAP, localdir); err != nil {
		t.Error(err)
	}

	// Import tile.dim
	if err := storage.ImportLayer(ctx, tile, layer, ExtensionDIMAP, localdir2); err != nil {
		t.Error(err)
	}

	// Delete tile.dim
	if err := storage.DeleteLayer(ctx, tile, layer, ExtensionDIMAP); err != nil {
		t.Error(err)
	}

	// Verif
	if _, err := os.Stat(path.Join(localdir2, LayerFileName(tile, layer, ExtensionDIMAP))); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(path.Join(localdir2, LayerFileName(tile, layer, ExtensionDIMAPData), "data")); err != nil {
		t.Error(err)
	}

	// Save tile.tif
	if _, err := storage.SaveLayer(ctx, tile, layer, ExtensionGTiff, localdir); err != nil {
		t.Error(err)
	}

	// Import tile.tif
	if err := storage.ImportLayer(ctx, tile, layer, ExtensionGTiff, localdir2); err != nil {
		t.Error(err)
	}

	// Delete tile.dim
	if err := storage.DeleteLayer(ctx, tile, layer, ExtensionGTiff); err != nil {
		t.Error(err)
	}

	// Verif
	if _, err := os.Stat(path.Join(localdir2, LayerFileName(tile, layer, ExtensionGTiff))); err != nil {
		t.Error(err)
	}
}
