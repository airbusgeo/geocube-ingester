package provider

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/airbusgeo/geocube-ingester/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	landsatAwsBucket         = "usgs-landsat"
	landsatAwsPrefixTemplate = "collection02/level-1/standard/%s/%s/%s/%s/%s/"
	landsatAwsRegion         = "us-west-2"
)

// LandsatAwsImageProvider implements ImageProvider for LandsatAws
type LandsatAwsImageProvider struct {
	accessKeyId     string
	secretAccessKey string
}

// Name implements ImageProvider
func (ip *LandsatAwsImageProvider) Name() string {
	return "LandsatAws"
}

// NewLandsatAwsImageProvider creates a new ImageProvider from LandsatAws
func NewLandsatAwsImageProvider(accessKeyId, secretAccessKey string) *LandsatAwsImageProvider {
	return &LandsatAwsImageProvider{accessKeyId, secretAccessKey}

}

// Download implements ImageProvider
func (ip *LandsatAwsImageProvider) Download(ctx context.Context, scene common.Scene, localDir string) error {

	sceneName := scene.SourceID
	switch common.GetConstellationFromProductId(sceneName) {
	case common.Landsat89:
	default:
		return fmt.Errorf("LandsatAwsImageProvider: constellation not supported")
	}

	info, err := common.Info(sceneName)
	if err != nil {
		return fmt.Errorf("LandsatAwsImageProvider.common.Info: %w", err)
	}

	sensorCollection := info["COLLECTION"]
	landsatPath := info["PATH"]
	landsatRow := info["ROW"]
	year := info["YEAR"]

	landsatAwsPrefix := fmt.Sprintf(landsatAwsPrefixTemplate, sensorCollection, year, landsatPath, landsatRow, sceneName)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ip.accessKeyId, ip.secretAccessKey, "")),
		config.WithRegion(landsatAwsRegion),
	)

	if err != nil {
		return fmt.Errorf("LandsatAwsImageProvider config.LoadDefaultConfig: %w", err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(
		cfg,
	)

	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024 // 10MB per part
	})

	// Get the first page of results for ListObjectsV2 for a bucket
	paginator := s3.NewListObjectsV2Paginator(client,
		&s3.ListObjectsV2Input{
			Bucket:       aws.String(landsatAwsBucket),
			Prefix:       aws.String(landsatAwsPrefix),
			RequestPayer: "requester",
		},
		func(o *s3.ListObjectsV2PaginatorOptions) {
			o.Limit = 200 // much more than the the typical number of files in a Landsat product (i.e. the pagination mechanism exists but is expected to process only one page)
		},
	)

	// create product directory
	productDir := path.Join(localDir, sceneName)
	err = os.MkdirAll(productDir, 0755)
	if err != nil {
		return fmt.Errorf("LandsatAwsImageProvider os.MkdirAll: %w", err)
	}

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return fmt.Errorf("LandsatAwsImageProvider paginator.NextPage: %w", err)
		}

		for _, object := range page.Contents {
			//log.Printf("key=%s size=%d", aws.ToString(object.Key), *object.Size)
			objectKey := aws.ToString(object.Key)
			objectFileName := objectKey[strings.LastIndex(objectKey, "/")+1:]
			localFilePath := path.Join(productDir, objectFileName)

			err := downloadSingleObjectToFile(downloader, ctx, landsatAwsBucket, objectKey, localFilePath)
			if err != nil {
				return fmt.Errorf("LandsatAwsImageProvider.%w", err)
			}
		}
	}

	return nil
}

func downloadSingleObjectToFile(downloader *manager.Downloader, ctx context.Context, bucketName string, objectKey string, localPath string) error {
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("downloadSingleObjectToFile: failed to create file %s: %w", localPath, err)
	}
	defer file.Close()

	_, err = downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String(objectKey),
		RequestPayer: "requester",
	})
	if err != nil {
		return fmt.Errorf("downloadSingleObjectToFile: failed to download object %s:%s: %w",
			bucketName, objectKey, err)
	}

	return nil
}
