package catalog

import (
	"context"
	"encoding/xml"
	"fmt"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/catalog"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/creodias"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/gcs"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/paulsmith/gogeos/geos"
	"golang.org/x/sync/errgroup"
)

func sceneBurstsInventory(ctx context.Context, scene *entities.Scene, pareaAOI *geos.PGeometry, annotationsProviders []catalog.AnnotationsProvider) error {
	log.Logger(ctx).Debug("Load annotations of " + scene.SourceID)
	bursts, err := burstsFromAnnotations(ctx, scene, annotationsProviders)
	if err != nil {
		return err
	}
	// Check that burst AOI intersects area AOI
	for _, burst := range bursts {
		burstAOI, err := geos.FromWKT(burst.GeometryWKT)
		if err != nil {
			return fmt.Errorf("burstsInventory.FromWKT: %w", err)
		}
		intersect, err := pareaAOI.Intersects(burstAOI)
		if err != nil {
			return fmt.Errorf("burstsInventory.Intersects: %w", err)
		}
		if intersect {
			// Add burst to scene
			scene.Tiles = append(scene.Tiles, burst)
			scene.Data.TileMappings[burst.SourceID] = common.TileMapping{SwathID: burst.Data.SwathID, TileNr: burst.Data.TileNr}
		}
	}
	log.Logger(ctx).Sugar().Debugf("Found %d bursts intersecting aoi in "+scene.SourceID, len(scene.Tiles))
	return nil
}

func sceneBurstsInventoryWorker(ctx context.Context, jobs <-chan *entities.Scene, pareaAOI *geos.PGeometry, annotationsProviders []catalog.AnnotationsProvider) error {
	for scene := range jobs {
		select {
		case <-ctx.Done():
		default:
			if err := sceneBurstsInventory(ctx, scene, pareaAOI, annotationsProviders); err != nil {
				return err
			}
		}
	}
	return nil
}

// BurstsInventory creates an inventory of all the bursts of the given scenes
// Returns the number of bursts
func (c *Catalog) BurstsInventory(ctx context.Context, aoi geos.Geometry, scenes []*entities.Scene) ([]*entities.Scene, int, error) {
	// Prepare geometry for intersection
	areaAOI, err := aoi.Buffer(0.05)
	if err != nil {
		return nil, 0, fmt.Errorf("burstsInventory.Buffer: %w", err)
	}
	pareaAOI := areaAOI.Prepare()

	// Create group
	wg, ctx := errgroup.WithContext(ctx)
	jobChan := make(chan *entities.Scene, len(scenes))

	var annotationsProviders []catalog.AnnotationsProvider
	if c.GCStorageURL != "" {
		annotationsProviders = append(annotationsProviders, gcs.AnnotationsProvider{Bucket: c.GCStorageURL})
	}
	annotationsProviders = append(annotationsProviders, creodias.AnnotationsProvider{})

	// Start 10 workers
	for i := 0; i < 10 && i < len(scenes); i++ {
		wg.Go(func() error { return sceneBurstsInventoryWorker(ctx, jobChan, pareaAOI, annotationsProviders) })
	}

	// Push jobs
	for _, scene := range scenes {
		jobChan <- scene
	}
	close(jobChan)

	// Wait
	if err := wg.Wait(); err != nil {
		return nil, 0, fmt.Errorf("burstsInventory.%w", err)
	}
	runtime.KeepAlive(areaAOI)

	// Filter empty scenes and get number of bursts
	n, j := 0, 0
	for i := range scenes {
		if t := len(scenes[i].Tiles); t != 0 {
			scenes[j] = scenes[i]
			n += t
			j++
		}
	}

	return scenes[0:j], n, nil
}

// BurstsSort defines for each burst the previous and reference bursts
// Returns the number of tracks
func (c *Catalog) BurstsSort(ctx context.Context, scenes []*entities.Scene) int {
	// Sort bursts by track
	burstsPerTrack := map[string][]*entities.Tile{}
	for _, scene := range scenes {
		for _, burst := range scene.Tiles {
			track := burst.SourceID[0:4]
			burstsPerTrack[track] = append(burstsPerTrack[track], burst)
		}
	}

	// Find previous and reference for each bursts
	for _, bursts := range burstsPerTrack {
		// Sort by AnxTime
		sort.Slice(bursts, func(i, j int) bool { return bursts[i].AnxTime < bursts[j].AnxTime })
		// Create pools of burst with similar AnxTime, sort by date and find ref and prev burst
		for i, il := 0, 0; i < len(bursts); il = i {
			for ; i < len(bursts) && bursts[i].AnxTime < bursts[il].AnxTime+5; i++ {
			}

			sbursts := bursts[il:i]
			sort.Slice(sbursts, func(j, k int) bool { return sbursts[j].Date.Before(sbursts[k].Date) })

			for j := 1; j < len(sbursts); j++ {
				b := sbursts[j]
				if !b.Ingested {
					b.Reference = &sbursts[0].TileLite
					b.Previous = &sbursts[j-1].TileLite
					// If the current date is more than 6 days after the previous date, log a warning
					if b.Date.After(b.Previous.Date.Add(time.Hour * 24 * 7)) {
						log.Logger(ctx).Sugar().Warnf("%s:%s No burst was found 6 days before. Found %s (%v before)",
							b.SceneID, b.SourceID, b.Previous.SceneID, b.Date.Sub(b.Previous.Date))
					}
				}
			}
		}

	}

	return len(burstsPerTrack)
}

// burstsFromAnnotations loads bursts features (anxtime, swath and geometry) from annotation files
func burstsFromAnnotations(ctx context.Context, scene *entities.Scene, annotationsProviders []catalog.AnnotationsProvider) ([]*entities.Tile, error) {
	var err, e error
	var annotationsFiles map[string][]byte
	for _, annotationsProvider := range annotationsProviders {
		annotationsFiles, e = annotationsProvider.AnnotationsFiles(ctx, scene)
		if err = service.MergeErrors(false, err, e); err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("burstsFromAnnotations.%w", err)
	}

	var burstsInventory []*entities.Tile
	anxTimes := map[int]struct{}{}
	for url, file := range annotationsFiles {
		bursts, err := burstsFromAnnotation(file, url)
		if err != nil {
			return nil, fmt.Errorf("burstsFromAnnotations.%w", err)
		}
		for anxTime, burst := range bursts {
			if _, ok := anxTimes[anxTime]; !ok {
				anxTimes[anxTime] = struct{}{}
				// Add info from scene
				burst.Date = scene.Scene.Data.Date
				burst.SceneID = scene.SourceID
				burst.SourceID = fmt.Sprintf("%s%s_%s_%d", scene.Tags[common.TagOrbitDirection][0:1], scene.Tags[common.TagRelativeOrbit], burst.Data.SwathID, burst.AnxTime)
				burstsInventory = append(burstsInventory, burst)
			}
		}
	}

	return burstsInventory, nil
}

func burstsFromAnnotation(annotationFile []byte, annotationURL string) (map[int]*entities.Tile, error) {
	// XML GridPoint structure
	type GridPoint struct {
		Pixel     int     `xml:"pixel"`
		Line      int     `xml:"line"`
		Latitude  float64 `xml:"latitude"`
		Longitude float64 `xml:"longitude"`
	}

	// Read annotations file
	annotation := struct {
		XMLName         xml.Name  `xml:"product"`
		Swath           string    `xml:"adsHeader>swath"`
		LinesPerBurst   int       `xml:"swathTiming>linesPerBurst"`
		SamplesPerBurst int       `xml:"swathTiming>samplesPerBurst"`
		AzimuthAnxTime  []float64 `xml:"swathTiming>burstList>burst>azimuthAnxTime"`

		GridPoint []GridPoint `xml:"geolocationGrid>geolocationGridPointList>geolocationGridPoint"`
	}{}
	if err := xml.Unmarshal(annotationFile, &annotation); err != nil {
		return nil, fmt.Errorf("readAnnotation.Unmarshal[%s] : %w", annotationFile, err)
	}

	// Position of firsts and last points
	first := map[int]GridPoint{}
	last := map[int]GridPoint{}
	for _, point := range annotation.GridPoint {
		if point.Pixel == 0 {
			first[point.Line] = point
		} else if point.Pixel == annotation.SamplesPerBurst-1 {
			last[point.Line] = point
		}
	}

	// Burst
	bursts := map[int]*entities.Tile{}
	for i, anxTime := range annotation.AzimuthAnxTime {
		// First/Last lines of the burst
		firstline := i * annotation.LinesPerBurst
		lastline := (i + 1) * annotation.LinesPerBurst
		if _, ok := first[firstline]; !ok {
			firstline-- // -1 because first and lastline sometimes shifts by 1 for some reason?
			if _, ok := first[firstline]; !ok {
				return nil, fmt.Errorf("readAnnotation: First line not found in annotation file %s", annotationURL)
			}
		}
		if _, ok := last[lastline]; !ok {
			lastline-- // -1 because first and lastline sometimes shifts by 1 for some reason?
			if _, ok := last[lastline]; !ok {
				return nil, fmt.Errorf("readAnnotation: Last line not found in annotation file %s", annotationURL)
			}
		}

		// Set bursts
		intAnxTime := int(math.Round(math.Mod(anxTime, float64(12*24*60*60/175)) * 10))
		bursts[intAnxTime] = &entities.Tile{
			Data: common.TileAttrs{
				SwathID: annotation.Swath,
				TileNr:  i + 1,
			},
			AnxTime: intAnxTime,
			GeometryWKT: fmt.Sprintf("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
				first[firstline].Longitude, first[firstline].Latitude,
				first[lastline].Longitude, first[lastline].Latitude,
				last[lastline].Longitude, last[lastline].Latitude,
				last[firstline].Longitude, last[firstline].Latitude,
				first[firstline].Longitude, first[firstline].Latitude,
			),
		}
	}

	return bursts, nil
}
