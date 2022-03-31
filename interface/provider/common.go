package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/cavaliercoder/grab"
	"github.com/mholt/archiver"
)

// ErrProductNotFound is an error returned when a product is not found or available
type ErrProductNotFound struct {
	Product string
}

func (e ErrProductNotFound) Error() string {
	return fmt.Sprintf("Product not found or unavailable: %s", e.Product)
}

// constellation defines the kind of satellites
type constellation int

const (
	Unknown   constellation = iota
	Sentinel1 constellation = iota // MMM_BB_TTTR_LFPP_YYYYMMDDTHHMMSS_YYYMMDDTHHMMSS_OOOOOO_DDDDDD_CCCC.SAFE
	Sentinel2 constellation = iota // MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE or MMM_CCCC_FFFFDDDDDD_ssss_YYYYMMDDTHHMMSS_ROOO_VYYYYMMTDDHHMMSS_YYYYMMTDDHHMMSS.SAFE
)

func fmtBytes(bytes int64) string {
	v := float64(bytes)
	switch {
	case v > 1<<30:
		return fmt.Sprintf("%.2fGo", v/(1<<30))
	case v > 1<<20:
		return fmt.Sprintf("%.2fMo", v/(1<<20))
	case v > 1<<10:
		return fmt.Sprintf("%.2fko", v/(1<<10))
	default:
		return fmt.Sprintf("%.2fo", v)
	}
}

func displayProgress(ctx context.Context, prefix string, resp *grab.Response, progressPeriod float64) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	progress, lastBytes, seconds := 0.0, int64(0), int64(0)
	for {
		select {
		case <-t.C:
			seconds++
			if resp.Progress() > progress {
				log.Logger(ctx).Sugar().Debugf("%s: %.2f%% %s/%s (%s/s)", prefix, 100*resp.Progress(), fmtBytes(resp.BytesComplete()), fmtBytes(resp.Size), fmtBytes((resp.BytesComplete()-lastBytes)/seconds))
				seconds = 0
				progress += progressPeriod
				lastBytes = resp.BytesComplete()
			}

		case <-resp.Done:
			return
		}
	}
}

func downloadZipWithAuth(ctx context.Context, url, localDir, sceneName, provider string, user, pword *string, header_key string, header_value *string) error {
	localZip := sceneFilePath(localDir, sceneName, service.ExtensionZIP)
	req, err := grab.NewRequest(localZip, url)
	if err != nil {
		return fmt.Errorf("downloadZipWithAuth.NewRequest: %w", err)
	}
	req = req.WithContext(ctx)

	// If Basic Auth
	if user != nil && pword != nil {
		req.HTTPRequest.SetBasicAuth(*user, *pword)
	}

	// If key/val Auth
	if header_value != nil {
		req.HTTPRequest.Header.Add(header_key, *header_value)
	}

	if err := download(ctx, req, provider+":"+sceneName); err != nil {
		return fmt.Errorf("downloadZipWithAuth.%w", err)
	}

	defer os.Remove(localZip)
	if err := unarchive(localZip, localDir); err != nil {
		return fmt.Errorf("downloadZipWithAuth.Unarchive: %w", err)
	}
	return nil
}

// download a file with display every 5%
func download(ctx context.Context, req *grab.Request, displayPrefix string) error {
	client := grab.NewClient()
	resp := client.Do(req)

	displayProgress(ctx, displayPrefix, resp, 0.05)

	if err := resp.Err(); err != nil {
		err = fmt.Errorf("download[%s]: %w", req.URL(), err)
		if resp.HTTPResponse == nil {
			return service.MakeTemporary(err)
		}
		switch resp.HTTPResponse.StatusCode {
		case 408, 429, 500, 501, 502, 503, 504:
			return service.MakeTemporary(err)
		default:
			return err
		}
	}
	return nil
}

// unarchive file with basic check. All errors are temporary.
func unarchive(localZip, localDir string) error {
	tmpdir, err := ioutil.TempDir(localDir, filepath.Base(localZip))
	if err != nil {
		return service.MakeTemporary(err)
	}
	defer os.RemoveAll(tmpdir)
	if err := archiver.Unarchive(localZip, tmpdir); err != nil {
		return service.MakeTemporary(err)
	}
	files, err := ioutil.ReadDir(tmpdir)
	if err != nil {
		return service.MakeTemporary(err)
	}
	if len(files) == 0 {
		return service.MakeTemporary(fmt.Errorf("empty zip"))
	}
	for _, f := range files {
		os.Rename(filepath.Join(tmpdir, f.Name()), filepath.Join(localDir, f.Name()))
	}
	return nil
}

// sceneFilePath returns the path of the scene, given the directory and the sceneid
func sceneFilePath(dir, sceneID string, ext service.Extension) string {
	return path.Join(dir, sceneID+"."+string(ext))
}

func getDownloadURL(searchURL string) (string, error) {
	resp, err := http.Get(searchURL)
	if err != nil {
		return "", fmt.Errorf("getDownloadURL.Get: %w", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("getDownloadURL.ReadAll: %w", err)
	}
	defer resp.Body.Close()

	jsonURL := struct {
		Features []struct {
			Properties struct {
				Services struct {
					Download struct {
						URL string `json:"url"`
					} `json:"download"`
				} `json:"services"`
			} `json:"properties"`
		} `json:"features"`
	}{}

	if err := json.Unmarshal(body, &jsonURL); err != nil || len(jsonURL.Features) == 0 {
		if err == nil {
			return "", ErrProductNotFound{}
		}
		return "", fmt.Errorf("getDownloadURL.Unmarshal [%s]: %w", body, err)
	}

	return jsonURL.Features[0].Properties.Services.Download.URL, nil
}

func getConstellation(sceneName string) constellation {
	if strings.HasPrefix(sceneName, "S1") {
		return Sentinel1
	}
	if strings.HasPrefix(sceneName, "S2") {
		return Sentinel2
	}
	return Unknown
}

func getDate(sceneName string) (time.Time, error) {
	switch getConstellation(sceneName) {
	case Sentinel1:
		// MMM_BB_TTTR_LFPP_YYYYMMDDTHHMMSS_YYYMMDDTHHMMSS_OOOOOO_DDDDDD_CCCC.SAFE
		return time.Parse("20060102T150405", sceneName[17:32])
	case Sentinel2:
		// MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
		if t, err := time.Parse("20060102150405", sceneName[11:25]); err == nil {
			return t, err
		}
		// MMM_CCCC_FFFFDDDDDD_ssss_YYYYMMDDTHHMMSS_ROOO_VYYYYMMTDDHHMMSS_YYYYMMTDDHHMMSS.SAFE
		return time.Parse("20060102T150405", sceneName[25:40])
	}
	return time.Time{}, fmt.Errorf("Unable to extract date from " + sceneName)
}
