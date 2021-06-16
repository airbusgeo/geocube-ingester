package provider

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube/interface/storage/gcs"
	"google.golang.org/api/iterator"
)

// GSImageProvider implements ImageProvider for Google Storage Sentinel2 and LANDSAT buckets
type GSImageProvider struct {
	buckets map[constellation]string
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
func (ip *GSImageProvider) Download(ctx context.Context, sceneName, sceneUUID, localDir string) error {
	constellation := getConstellation(sceneName)
	bucket, ok := ip.buckets[constellation]
	if constellation == Unknown || !ok {
		return fmt.Errorf("GSImageProvider: constellation not supported")
	}
	var format map[string]string
	switch constellation {
	case Sentinel2:
		//MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
		//MMM_CCCC_FFFFDDDDDD_ssss_YYYYMMDDTHHMMSS_ROOO_VYYYYMMTDDHHMMSS_YYYYMMTDDHHMMSS.SAFE
		// /tiles/UTM_ZONE/LATITUDE_BAND/GRID_SQUARE/GRANULE_ID/
		format = map[string]string{
			"SCENE":         sceneName,
			"MISSION_ID":    sceneName[0:3],
			"PRODUCT_LEVEL": sceneName[7:10],
			"DATE":          sceneName[11:19],
			"YEAR":          sceneName[11:15],
			"MONTH":         sceneName[15:17],
			"TIME":          sceneName[19:25],
			"DAY":           sceneName[17:19],
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

	if _, err := ip.downloadDirectory(ctx, url, localDir); err != nil {
		return fmt.Errorf("GSImageProvider.%w", err)
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

	gs, derr := gcs.NewGsStrategy(ctx)
	if derr != nil {
		err = fmt.Errorf("downloadDirectory: %w", err)
		return
	}

	bucket, prefix, derr := gcs.Parse(uri)
	if derr != nil {
		err = fmt.Errorf("downloadDirectory: %w", err)
		return
	}
	if len(bucket) == 0 {
		err = fmt.Errorf("missing bucket")
		return
	}
	prefix = strings.TrimRight(prefix, "/")
	if dstDir == "" {
		dstDir, err = ioutil.TempDir("", "gcs")
		if err != nil {
			err = fmt.Errorf("ioutil.tempdir: %w", err)
			return
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
				if err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case uri, ok := <-downloads:
					if !ok {
						return
					}
					//log.Printf("worker %d download gs://%s/%s to %s", i, uri.bucket, uri.object, uri.file)
					derr := gs.DownloadToFile(ctx, uri.bucket+"/"+uri.object, uri.file)
					if derr != nil {
						err = derr
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
		log.Fatal(err)
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
			err = fmt.Errorf("bucket iterate: %w", iterr)
			close(downloads)
			return
		}
		if objectAttrs.Prefix != "" {
			//log.Printf("make dir %s", filepath.Join(dstDirectory, objectAttrs.Prefix))
			mkdir := filepath.Join(dstDir, objectAttrs.Prefix)
			ferr := os.MkdirAll(mkdir, 0700)
			if ferr != nil {
				close(downloads)
				err = fmt.Errorf("mkdirall %s: %w", mkdir, ferr)
				return
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
			ferr := os.MkdirAll(dirname, 0700)
			if ferr != nil {
				close(downloads)
				err = fmt.Errorf("mkdirall %s: %w", dirname, ferr)
				return
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
