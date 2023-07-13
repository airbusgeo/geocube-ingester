package url

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	osioGcs "github.com/airbusgeo/osio/gcs"
	osioS3 "github.com/airbusgeo/osio/s3"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube/interface/storage/uri"
	"github.com/airbusgeo/osio"
)

type AnnotationsProvider struct {
	URLPattern string
}

// AnnotationFiles retrieves them from an archive in GCS
func (ap AnnotationsProvider) AnnotationsFiles(ctx context.Context, scene *entities.Scene) (map[string][]byte, error) {
	reg, err := regexp.Compile(scene.SourceID + ".SAFE/annotation/s1[^/]*xml")
	if err != nil {
		return nil, fmt.Errorf("annotationFiles.Compile[%s]: %w", scene.SourceID+".SAFE/annotation/*xml", err)
	}

	info, err := common.Info(scene.SourceID)
	if err != nil {
		return nil, fmt.Errorf("AnnotationsFiles.Info: %w", err)
	}
	file := common.FormatBrackets(ap.URLPattern, info)
	annotationsFiles, err := extract(ctx, file, *reg)
	if err != nil {
		return nil, fmt.Errorf("annotationsFiles[%s].%w", file, err)
	}
	return annotationsFiles, nil
}

// extract files from archive in gcs
func extract(ctx context.Context, file string, reg regexp.Regexp) (map[string][]byte, error) {
	uri, err := uri.ParseUri(file)
	if err != nil {
		return nil, fmt.Errorf("ParseURI: %w", err)
	}
	var reader io.ReaderAt
	var size int64
	protocol := strings.ToLower(uri.Protocol())
	switch protocol {
	case "file", "":
		info, err := os.Stat(file)
		if err != nil {
			return nil, fmt.Errorf("os.Open: %w", err)
		}
		if info.IsDir() {
			return scanDir(ctx, file, reg)
		}

		obj, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("os.Open: %w", err)
		}
		defer obj.Close()

		// get the file size
		stat, err := obj.Stat()
		if err != nil {
			return nil, fmt.Errorf("obj.Stat: %w", err)
		}
		reader = obj
		size = stat.Size()
	case "gs", "s3":
		var handler osio.KeyStreamerAt
		if protocol == "gs" {
			handler, err = osioGcs.Handle(ctx)
			if err != nil {
				return nil, fmt.Errorf("extract.GSHandle: %w", err)
			}
		} else {
			handler, err = osioS3.Handle(ctx)
			if err != nil {
				return nil, fmt.Errorf("extract.S3Handle: %w", err)
			}
		}
		adapter, err := osio.NewAdapter(handler)
		if err != nil {
			return nil, fmt.Errorf("extract.NewAdapter: %w", err)
		}
		obj, err := adapter.Reader(path.Join(uri.Bucket(), uri.Path()))
		if err != nil {
			return nil, fmt.Errorf("extract.Reader: %w", err)
		}
		reader = obj
		size = obj.Size()
	default:
		return nil, fmt.Errorf("failed to determine storage strategy")
	}

	zipf, err := zip.NewReader(reader, size)
	if err != nil {
		return nil, fmt.Errorf("extract.NewReader: : %w", err)
	}

	files := map[string][]byte{}
	for _, f := range zipf.File {
		if reg.MatchString(f.Name) {
			if err := func() error {
				fr, err := f.Open()
				if err != nil {
					return fmt.Errorf("open[%s]: %w", f.Name, err)
				}
				defer fr.Close()

				if files[f.Name], err = ReadAll(fr); err != nil {
					return fmt.Errorf("ReadAll[%s]: %w", f.Name, err)
				}
				return nil
			}(); err != nil {
				return nil, fmt.Errorf("extract.%w", err)
			}
		}
	}
	return files, nil
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func ReadAll(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 512)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}

// scanDir returns the files of the dir that match the reg
func scanDir(ctx context.Context, dir string, reg regexp.Regexp) (map[string][]byte, error) {
	files := map[string][]byte{}

	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && reg.MatchString(path) {
			if files[path], err = os.ReadFile(path); err != nil {
				return fmt.Errorf("ReadAll[%s]: %w", path, err)
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scanFile: %w", err)
	}
	return files, nil
}
