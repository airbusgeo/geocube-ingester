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

type Progress struct {
	ctx         context.Context
	prefix      string
	total       int64
	displayStep float64

	complete     int64
	lastComplete int64
	nextDisplay  int64
	timer        time.Time
}

func NewProgress(ctx context.Context, prefix string, total int64, displayStepPc float64) *Progress {
	return &Progress{
		ctx:         ctx,
		prefix:      prefix,
		total:       total,
		timer:       time.Now(),
		displayStep: displayStepPc / 100,
	}
}

func (p *Progress) Update(bytesComplete int64) {
	p.complete = bytesComplete
	if bytesComplete > p.nextDisplay {
		p.Display()
		p.nextDisplay += int64(p.displayStep * float64(p.total))
		p.lastComplete = bytesComplete
		p.timer = time.Now()
	}
}

func (p *Progress) UpdateDelta(bytesAdded int64) {
	p.Update(p.complete + bytesAdded)
}

func (p *Progress) Display() {
	log.Logger(p.ctx).Sugar().Debugf("%s: %.2f%% %s/%s (%s/s)", p.prefix, 100*float64(p.complete)/float64(p.total), fmtBytes(p.complete), fmtBytes(p.total),
		fmtBytes(int64(float64(p.complete-p.lastComplete)/time.Since(p.timer).Seconds())))
}

func displayProgress(ctx context.Context, prefix string, resp *grab.Response, everyPc float64) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	progress := NewProgress(ctx, prefix, resp.Size, everyPc)
	for {
		select {
		case <-t.C:
			progress.Update(resp.BytesComplete())

		case <-resp.Done:
			return
		}
	}
}

func downloadZipWithAuth(ctx context.Context, url, localDir, sceneName, provider string, user, pword *string, header_key string, header_value *string, copyAuthOnRedirect bool) error {
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

	if err := download(ctx, req, provider+":"+sceneName, copyAuthOnRedirect); err != nil {
		return fmt.Errorf("downloadZipWithAuth.%w", err)
	}

	defer os.Remove(localZip)
	if err := unarchive(ctx, localZip, localDir); err != nil {
		return service.MakeTemporary(fmt.Errorf("downloadZipWithAuth.Unarchive: %w", err))
	}
	return nil
}

func checkRedirectAndCopyAuth(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return fmt.Errorf("stopped after 10 redirects")
	}
	if auth, ok := via[0].Header["Authorization"]; ok {
		req.Header.Add("Authorization", auth[0])
	}
	return nil
}

// download a file with display every 5%
func download(ctx context.Context, req *grab.Request, displayPrefix string, copyAuthOnRedirect bool) error {
	client := grab.NewClient()
	if copyAuthOnRedirect {
		client.HTTPClient.CheckRedirect = checkRedirectAndCopyAuth
	}
	resp := client.Do(req)

	displayProgress(ctx, displayPrefix, resp, 5)

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

// unarchive file with basic check.
func unarchive(ctx context.Context, localZip, localDir string) error {
	tmpdir, err := ioutil.TempDir(localDir, filepath.Base(localZip))
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)
	if err := archiver.Unarchive(localZip, tmpdir); err != nil {
		return fmt.Errorf("during unarchiving: %w", err)
	}
	files, err := os.ReadDir(tmpdir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("empty zip")
	}
	for i, f := range files {
		if err = os.Rename(filepath.Join(tmpdir, f.Name()), filepath.Join(localDir, f.Name())); err != nil {
			for j := 0; j < i; j++ {
				os.RemoveAll(filepath.Join(localDir, files[j].Name()))
			}
			return err
		}
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
