package provider

import (
	"context"
	"os"
	"testing"

	"github.com/airbusgeo/geocube-ingester/common"
)

func testDownload(t *testing.T) {
	awsAccessKeyId := os.Getenv("LANDSAT_AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("LANDSAT_AWS_SECRET_ACCESS_KEY")

	ip := LandsatAwsImageProvider{accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey}

	scene := common.Scene{SourceID: "LC09_L1GT_166003_20250603_20250603_02_T2"}

	err := ip.Download(context.Background(), scene, os.TempDir())
	if err != nil {
		t.Fatalf("Failed to Download product: %v", err)
	}
}

func TestDownloadLandsatAWS(t *testing.T) {
	//testDownload(t)
}
