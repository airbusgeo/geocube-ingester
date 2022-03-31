package provider

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
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
	buckets map[constellation]string
}

// Name implements ImageProvider
func (ip *GSImageProvider) Name() string {
	return "GoogleStorage"
}

// NewGSImageProvider creates a new ImageProvider from Google Storage Sentinel2 and LANDSAT buckets
func NewGSImageProvider() *GSImageProvider {
	return &GSImageProvider{buckets: map[constellation]string{}}
}

// AddBucket to the provider
// constellation must be one of sentinel1, sentinel-1, sentinel2, sentinel-2
// bucket can contain several {IDENTIFIER} than will be replaced according to the information found in scenename
// IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)
func (ip *GSImageProvider) AddBucket(constellation, bucket string) error {
	switch strings.ToLower(constellation) {
	case "sentinel1", "sentinel-1":
		ip.buckets[Sentinel1] = bucket
	case "sentinel2", "sentinel-2":
		ip.buckets[Sentinel2] = bucket
	default:
		return fmt.Errorf("GSImageProvider: constellation not supported")
	}
	return nil
}

// Download implements ImageProvider
func (ip *GSImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	sceneName := scene.SourceID
	constellation := getConstellation(sceneName)
	bucket, ok := ip.buckets[constellation]
	if constellation == Unknown || !ok {
		return fmt.Errorf("GSImageProvider: constellation not supported")
	}
	var format map[string]string
	switch constellation {
	case Sentinel1:
		// MMM_BB_TTTR_LFPP_YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_OOOOOO_DDDDDD_CCCC.SAFE
		format = map[string]string{
			"SCENE":         sceneName,
			"MISSION_ID":    sceneName[0:3],
			"MODE":          sceneName[4:6],
			"PRODUCT_TYPE":  sceneName[7:10],
			"RESOLUTION":    sceneName[10:11],
			"PRODUCT_LEVEL": sceneName[12:13],
			"PRODUCT_CLASS": sceneName[13:14],
			"POLARISATION":  sceneName[14:16],
			"DATE":          sceneName[17:25],
			"YEAR":          sceneName[17:21],
			"MONTH":         sceneName[21:23],
			"DAY":           sceneName[23:25],
			"TIME":          sceneName[26:32],
			"HOUR":          sceneName[26:28],
			"MINUTE":        sceneName[28:30],
			"SECOND":        sceneName[30:32],
			"ORBIT":         sceneName[49:55],
			"MISSION":       sceneName[56:62],
			"UNIQUE_ID":     sceneName[63:67],
		}
	case Sentinel2:
		//MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
		//MMM_CCCC_FFFFDDDDDD_ssss_YYYYMMDDTHHMMSS_ROOO_VYYYYMMTDDHHMMSS_YYYYMMTDDHHMMSS.SAFE
		format = map[string]string{
			"SCENE":         sceneName,
			"MISSION_ID":    sceneName[0:3],
			"PRODUCT_LEVEL": sceneName[7:10],
			"DATE":          sceneName[11:19],
			"YEAR":          sceneName[11:15],
			"MONTH":         sceneName[15:17],
			"DAY":           sceneName[17:19],
			"TIME":          sceneName[19:25],
			"HOUR":          sceneName[19:21],
			"MINUTE":        sceneName[21:23],
			"SECOND":        sceneName[23:25],
			"PDGS":          sceneName[27:31],
			"ORBIT":         sceneName[33:36],
			"TILE":          sceneName[38:43],
			"LATITUDE_BAND": sceneName[38:40],
			"GRID_SQUARE":   sceneName[40:41],
			"GRANULE_ID":    sceneName[41:43],
		}
	default:
		return fmt.Errorf("GSImageProvider: constellation not supported")
	}

	url := bucket
	for k, v := range format {
		url = strings.ReplaceAll(url, "{"+k+"}", v)
	}

	if filepath.Ext(url) == "."+string(service.ExtensionZIP) {
		if err := ip.downloadZip(ctx, url, localDir); err != nil {
			return fmt.Errorf("GSImageProvider[%s].%w", url, err)
		}
	} else if _, err := ip.downloadDirectory(ctx, url, localDir); err != nil {
		return fmt.Errorf("GSImageProvider[%s].%w", url, err)
	}
	return nil
}

//downloadDirectory fetches all objects prefixed by uri to destination
//It returns the list of absolute filenames that were created (i.e with the destination prefix)
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
		dstDir, err = ioutil.TempDir("", "gcs")
		if err != nil {
			return nil, fmt.Errorf("ioutil.tempdir: %w", err)
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

//downloadZip to destination
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
	if err := unarchive(localZip, dstDir); err != nil {
		return fmt.Errorf("downloadZip.Unarchive: %w", err)
	}
	return nil
}
