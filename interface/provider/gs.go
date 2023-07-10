package provider

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube/interface/storage/gcs"
	"google.golang.org/api/iterator"
)

// GSImageProvider implements ImageProvider for Google Storage Sentinel2 and LANDSAT buckets
type GSImageProvider struct {
	buckets map[common.Constellation][]string
}

// Name implements ImageProvider
func (ip *GSImageProvider) Name() string {
	return "GoogleStorage"
}

// NewGSImageProvider creates a new ImageProvider from Google Storage Sentinel2 and LANDSAT buckets
func NewGSImageProvider() *GSImageProvider {
	return &GSImageProvider{buckets: map[common.Constellation][]string{}}
}

// AddBucket to the provider
// constellation must be one of sentinel1, sentinel-1, sentinel2, sentinel-2
// bucket can contain several {IDENTIFIER} than will be replaced according to the information found in scenename
// IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)
func (ip *GSImageProvider) AddBucket(constellation, bucket string) error {
	switch strings.ToLower(constellation) {
	case "sentinel1", "sentinel-1":
		ip.buckets[common.Sentinel1] = append(ip.buckets[common.Sentinel1], bucket)
	case "sentinel2", "sentinel-2":
		ip.buckets[common.Sentinel2] = append(ip.buckets[common.Sentinel2], bucket)
	case "spot":
		ip.buckets[common.SPOT] = append(ip.buckets[common.SPOT], bucket)
	case "phr":
		ip.buckets[common.PHR] = append(ip.buckets[common.PHR], bucket)
	default:
		return fmt.Errorf("GSImageProvider: constellation not supported")
	}
	return nil
}

func findBlob(ctx context.Context, url string) (string, error) {
	// Find the first blob that matches the url pattern
	bucket, blob, err := gcs.Parse(url)
	if err != nil {
		return "", err
	}
	gsClient, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}
	// Create a regexp from blob, replacing "*" by ".*" and "?" by "."
	blobRe := strings.ReplaceAll(strings.ReplaceAll(regexp.QuoteMeta(blob), "\\*", ".*"), "\\?", ".")
	re, err := regexp.Compile(blobRe)
	if err != nil {
		return "", fmt.Errorf("compile[%s]: %w", blobRe, err)
	}
	// Extract the prefix
	if i := strings.Index(blob, "*"); i != -1 {
		blob = blob[:i]
	}
	// Find all the blobs that match the prefix
	it := gsClient.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: blob})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", fmt.Errorf("list[%s/%s*]: %w", bucket, blob, err)
		}
		if idx := re.FindIndex([]byte(attrs.Name)); idx != nil && idx[0] == 0 {
			return "gs://" + bucket + "/" + attrs.Name[:idx[1]], nil
		}
	}
	return url, ErrProductNotFound{url}
}

// Download implements ImageProvider
func (ip *GSImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	constellation := common.GetConstellationFromProductId(sceneName)
	buckets, ok := ip.buckets[constellation]
	if constellation == common.Unknown || !ok {
		return fmt.Errorf("GSImageProvider: constellation not supported")
	}
	format, err := common.Info(sceneName)
	if err != nil {
		return fmt.Errorf("GSImageProvider: %w", err)
	}

	for _, bucket := range buckets {
		url := common.FormatBrackets(bucket, format)
		if strings.Contains(url, "*") {
			if url, err = findBlob(ctx, url); err != nil {
				return fmt.Errorf("GSImageProvider: %w", err)
			}
		}
		e := func() error {
			if filepath.Ext(url) == "."+string(service.ExtensionZIP) {
				if err := ip.downloadZip(ctx, url, localDir); err != nil {
					return fmt.Errorf("GSImageProvider[%s].%w", url, err)
				}
			} else if files, err := ip.downloadDirectory(ctx, url, filepath.Join(localDir, filepath.Base(url))); err != nil {
				return fmt.Errorf("GSImageProvider[%s].%w", url, err)
			} else if len(files) == 0 {
				return fmt.Errorf("GSImageProvider[%s]: not found", url)
			}
			return nil
		}()

		if err = service.MergeErrors(false, err, e); err == nil {
			break
		}
	}
	return err
}

// downloadDirectory fetches all objects prefixed by uri to destination
// It returns the list of absolute filenames that were created (i.e with the destination prefix)
func (ip *GSImageProvider) downloadDirectory(ctx context.Context, uri string, dstDir string) (files []string, err error) {
	defer func() {
		if err != nil {
			err = service.MakeTemporary(err)
		}
	}()

	gs, err := gcs.NewGsStrategy(ctx)
	if err != nil {
		return nil, fmt.Errorf("downloadDirectory: %w", err)
	}

	bucket, prefix, err := gcs.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("downloadDirectory: %w", err)
	}
	if len(bucket) == 0 {
		return nil, fmt.Errorf("missing bucket")
	}
	prefix = strings.TrimRight(prefix, "/")
	if dstDir == "" {
		dstDir, err = os.MkdirTemp("", "gcs")
		if err != nil {
			return nil, fmt.Errorf("os.MkdirTemp: %w", err)
		}
	}
	type gsUriToDownload struct {
		bucket, object string
		file           string
	}
	downloads := make(chan gsUriToDownload)
	ctx, cncl := context.WithCancel(ctx)
	defer cncl()
	wg := sync.WaitGroup{}
	concurrency := 5
	wg.Add(concurrency)
	filemu := sync.Mutex{}
	for worker := 0; worker < concurrency; worker++ {
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case uri, ok := <-downloads:
					if !ok {
						return
					}
					if err = gs.DownloadToFile(ctx, uri.bucket+"/"+uri.object, uri.file); err != nil {
						return
					}
					filemu.Lock()
					files = append(files, uri.file)
					filemu.Unlock()
				}
			}
		}(worker)
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	q := &storage.Query{Prefix: prefix, Versions: false}
	q.SetAttrSelection([]string{"Name"})
	it := client.Bucket(bucket).Objects(ctx, q)
	for {
		objectAttrs, iterr := it.Next()
		if iterr == iterator.Done {
			break
		}
		if iterr != nil {
			close(downloads)
			return nil, fmt.Errorf("bucket iterate: %w", iterr)
		}
		if objectAttrs.Prefix != "" {
			mkdir := filepath.Join(dstDir, objectAttrs.Prefix)
			ferr := os.MkdirAll(mkdir, 0766)
			if ferr != nil {
				close(downloads)
				return nil, fmt.Errorf("mkdirall %s: %w", mkdir, ferr)
			}
		} else {
			filename := objectAttrs.Name
			if strings.HasPrefix(objectAttrs.Name, prefix) {
				filename = objectAttrs.Name[len(prefix):]
			}
			if len(filename) > 0 && filename[len(filename)-1] == '/' {
				continue
			}
			dirname := filepath.Join(dstDir, filepath.Dir(filename))
			ferr := os.MkdirAll(dirname, 0766)
			if ferr != nil {
				close(downloads)
				return nil, fmt.Errorf("mkdirall %s: %w", dirname, ferr)
			}
			filename = filepath.Join(dstDir, filename)
			downloads <- gsUriToDownload{
				bucket: bucket,
				object: objectAttrs.Name,
				file:   filename,
			}
		}
	}
	close(downloads)
	wg.Wait()
	return
}

// downloadZip to destination
func (ip *GSImageProvider) downloadZip(ctx context.Context, uri string, dstDir string) error {
	gs, err := gcs.NewGsStrategy(ctx)
	if err != nil {
		return fmt.Errorf("downloadZip.NewGsStrategy: %w", err)
	}
	localZip := path.Join(dstDir, filepath.Base(uri))
	if err := gs.DownloadToFile(ctx, uri, localZip); err != nil {
		return fmt.Errorf("downloadZip.%w", err)
	}
	defer os.Remove(localZip)
	if err := unarchive(ctx, localZip, dstDir); err != nil {
		return service.MakeTemporary(fmt.Errorf("downloadZip.Unarchive: %w", err))
	}
	return nil
}
