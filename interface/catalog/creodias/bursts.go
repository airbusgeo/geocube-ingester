package creodias

import (
	"context"
	"encoding/xml"
	"fmt"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/service"
)

type AnnotationsProvider struct {
}

// AnnotationFiles retrieves them from Creodias
func (ap AnnotationsProvider) AnnotationsFiles(ctx context.Context, scene *entities.Scene) (map[string][]byte, error) {
	// Load annotations URL
	annotationsURLs, err := annotationsURLs(ctx, scene)
	if err != nil {
		return nil, fmt.Errorf("Creodias.AnnotationsFiles.%w", err)
	}

	// Download annotation file
	annotationsFiles := map[string][]byte{}
	for _, url := range annotationsURLs {
		if annotationsFiles[url], err = service.GetBodyRetry(url, 3); err != nil {
			return nil, fmt.Errorf("Creodias.AnnotationsFiles.GetBodyRetry: %w", err)
		}
	}
	return annotationsFiles, nil
}

// annotationsURLs retrieves the urls for the product annotation files
func annotationsURLs(ctx context.Context, scene *entities.Scene) ([]string, error) {
	scenePath := fmt.Sprintf("https://finder.creodias.eu/files/Sentinel-1/SAR/SLC/%s/%s.SAFE/", scene.Data.Date.Format("2006/01/02"), scene.SourceID)

	// Load scene manifest file
	manifestFile, err := service.GetBodyRetry(scenePath+"manifest.safe", 3)
	if err != nil {
		return nil, fmt.Errorf("annotationsURLs.getBodyRetry: %w", err)
	}

	// Read manifest to retrieve annotations urls
	manifest := struct {
		XMLName     xml.Name `xml:"XFDU"`
		Annotations []struct {
			RepID string `xml:"repID,attr"`
			URL   struct {
				Href string `xml:"href,attr"`
			} `xml:"byteStream>fileLocation"`
		} `xml:"dataObjectSection>dataObject"`
	}{}
	if err := xml.Unmarshal(manifestFile, &manifest); err != nil {
		return nil, fmt.Errorf("annotationsURLs.Unmarshal : %w (%s)", err, manifestFile)
	}

	var urls []string
	for _, annotation := range manifest.Annotations {
		if annotation.RepID == "s1Level1ProductSchema" && annotation.URL.Href != "" /*&& strings.Contains(annotation.URL.Href, "-vh-") speeds up the research, at risk*/ {
			urls = append(urls, scenePath+annotation.URL.Href)
		}
	}
	return urls, nil
}
