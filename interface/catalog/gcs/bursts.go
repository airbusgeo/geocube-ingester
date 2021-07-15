package gcs

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/osio"
	osioGcs "github.com/airbusgeo/osio/gcs"
)

type AnnotationsProvider struct {
	Bucket string
}

// AnnotationFiles retrieves them from an archive in GCS
func (ap AnnotationsProvider) AnnotationsFiles(ctx context.Context, scene *entities.Scene) (map[string][]byte, error) {
	reg, err := regexp.Compile(scene.SourceID + ".SAFE/annotation/s1[^/]*xml")
	if err != nil {
		return nil, fmt.Errorf("annotationFiles.Compile[%s]: %w", scene.SourceID+".SAFE/annotation/*xml", err)
	}

	file := path.Join(ap.Bucket, scene.SourceID+".zip")
	file = strings.ReplaceAll(file, "{PRODUCT_ID}", scene.SourceID)
	annotationsFiles, err := extract(ctx, file, *reg)
	if err != nil {
		return nil, fmt.Errorf("annotationsFiles[%s].%w", file, err)
	}
	return annotationsFiles, nil
}

// extract files from archive in gcs
func extract(ctx context.Context, gcsFile string, reg regexp.Regexp) (map[string][]byte, error) {
	gcsr, err := osioGcs.Handle(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract.GCSHandle: %w", err)
	}
	adapter, err := osio.NewAdapter(gcsr)
	if err != nil {
		return nil, fmt.Errorf("extract.NewAdapter: %w", err)
	}

	obj, err := adapter.Reader(gcsFile)
	if err != nil {
		return nil, fmt.Errorf("extract.Reader: %w", err)
	}

	zipf, err := zip.NewReader(obj, obj.Size())
	if err != nil {
		return nil, fmt.Errorf("extract.NewReader: : %w", err)
	}

	files := map[string][]byte{}
	for _, f := range zipf.File {
		if reg.MatchString(f.Name) {
			fr, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("extract.open[%s]: %w", f.Name, err)
			}
			defer fr.Close()

			if files[filepath.Base(f.Name)], err = ReadAll(fr); err != nil {
				return nil, fmt.Errorf("extract.ReadAll[%s]: %w", f.Name, err)
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
