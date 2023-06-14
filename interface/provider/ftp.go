package provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/jlaffaye/ftp"
)

// FTPImageProvider implements ImageProvider for connection to FTP
type FTPImageProvider struct {
	hote        string
	pathPattern string
	user        string
	pword       string
	tls         bool
}

// Name implements ImageProvider
func (ip *FTPImageProvider) Name() string {
	return "FTP"
}

// NewFTPImageProvider creates a new ImageProvider for ftp download link
// Example:
// hote: "ftp.example.org:21"
// pathPattern: full ftp path, including hote, port and folder tree. i.e: ftp://ftp.example.org:21/Images/{SCENE}.zip  (See github.com/airbusgeo/geocube-ingester/common : FormatBrackets)
func NewFTPImageProvider(pathPattern, user, pword string) *FTPImageProvider {
	if pathPattern[:6] == "ftp://" {
		pathPattern = pathPattern[6:]
	}
	splits := strings.SplitN(pathPattern, "/", 2)
	if len(splits) == 1 {
		splits = append(splits, "{SCENE}.zip")
	}
	splitHote := strings.SplitN(splits[0], ":", 2)
	tls := len(splitHote) == 2 && splitHote[1] == "990"

	return &FTPImageProvider{
		hote:        splits[0],
		tls:         tls,
		pathPattern: splits[1],
		user:        user,
		pword:       pword,
	}
}

// WriteCounter counts the number of bytes written to it. It implements to the io.Writer interface
// and we can pass this into io.TeeReader() which will report progress on each write cycle.
type WriteCounter struct {
	Progress *Progress
}

func (wc *WriteCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.Progress.UpdateDelta(int64(n))
	return n, nil
}

// Download implements ImageProvider
func (ip *FTPImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {
	format, err := common.Info(scene.SourceID)
	if err != nil {
		return fmt.Errorf("FTPImageProvider: %w", err)
	}

	path := common.FormatBrackets(ip.pathPattern, format)

	// Connection to FTP
	ftpOption := []ftp.DialOption{ftp.DialWithTimeout(5 * time.Second)}
	if ip.tls {
		ftpOption = append(ftpOption, ftp.DialWithTLS(&tls.Config{InsecureSkipVerify: true}))
	}
	c, err := ftp.Dial(ip.hote, ftpOption...)
	if err != nil {
		return fmt.Errorf("FTPImageProvider.Dial: %w", err)
	}

	if err = c.Login(ip.user, ip.pword); err != nil {
		return fmt.Errorf("FTPImageProvider.Login: %w", err)
	}
	defer c.Quit()

	// Get file size
	s, _ := c.FileSize(path)

	// Get file stream
	r, err := c.Retr(path)
	if err != nil {
		return fmt.Errorf("FTPImageProvider.Retr: %w", err)
	}
	defer r.Close()

	// Download to local file
	localZip := sceneFilePath(localDir, scene.SourceID, service.ExtensionZIP)
	destFile, err := os.Create(localZip)
	if err != nil {
		return fmt.Errorf("FTPImageProvider.Create: %w", err)
	}
	defer destFile.Close()
	defer os.Remove(localZip)

	_, err = io.Copy(destFile, io.TeeReader(r, &WriteCounter{Progress: NewProgress(ctx, "Ftp", s, 5)}))
	if err != nil {
		return fmt.Errorf("FTPImageProvider.Copy: %w", err)
	}

	// Unarchive
	if err := unarchive(ctx, localZip, localDir); err != nil {
		return service.MakeTemporary(fmt.Errorf("FTPImageProvider.Unarchive: %w", err))
	}
	return nil
}
