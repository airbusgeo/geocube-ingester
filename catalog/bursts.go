package catalog

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/catalog"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/annotations"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/url"
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
			retryCount := 3
			for {
				err := sceneBurstsInventory(ctx, scene, pareaAOI, annotationsProviders)
				if err == nil {
					break
				}
				if retryCount == 0 {
					log.Logger(ctx).Sugar().Errorf("%v", err)
					return err
				}
				log.Logger(ctx).Sugar().Warnf("Retry in 10sec: %v", err)

				retryCount--
				time.Sleep(10 * time.Second)
			}
		}
	}
	return nil
}

// BurstsInventory creates an inventory of all the bursts of the given scenes
// Returns the number of bursts
func (c *Catalog) BurstsInventory(ctx context.Context, area entities.AreaToIngest, aoi geos.Geometry, scenes []*entities.Scene) ([]*entities.Scene, int, error) {
	// Create group
	wg, ctx := errgroup.WithContext(ctx)
	jobChan := make(chan *entities.Scene, len(scenes))

	var annotationsProviders []catalog.AnnotationsProvider
	for _, annotationsUrl := range c.AnnotationsURLs {
		annotationsProviders = append(annotationsProviders, url.AnnotationsProvider{URLPattern: annotationsUrl})
	}
	for _, annotationsUrl := range area.AnnotationsURLs {
		annotationsProviders = append(annotationsProviders, url.AnnotationsProvider{URLPattern: annotationsUrl})
	}
	if len(annotationsProviders) == 0 {
		return nil, 0, fmt.Errorf("burstsInventory: no annotationProvider defined")
	}

	// Prepare geometry for intersection
	pareaAOI := aoi.Prepare()

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
	runtime.KeepAlive(aoi)

	// Filter empty scenes and get number of bursts
	n, j := 0, 0
	for i := range scenes {
		if t := len(scenes[i].Tiles); t != 0 {
			scenes[j] = scenes[i]
			n += t
			j++
		} else {
			log.Logger(ctx).Sugar().Infof("Remove empty scene: %s", scenes[i].SourceID)
		}
	}

	return scenes[0:j], n, nil
}

// BurstsSort defines for each burst the previous and reference bursts
// Returns the number of tracks and swaths
func (c *Catalog) BurstsSort(ctx context.Context, scenes []*entities.Scene) int {
	// Sort bursts by track and swath
	burstsPerTrackSwath := map[string][]*entities.Tile{}
	for _, scene := range scenes {
		for _, burst := range scene.Tiles {
			track := burst.SourceID[0:4]
			trackSwath := track + burst.Data.SwathID
			burstsPerTrackSwath[trackSwath] = append(burstsPerTrackSwath[trackSwath], burst)
		}
	}

	// Find previous and reference for each bursts
	for track, bursts := range burstsPerTrackSwath {
		// Sort by AnxTime
		sort.Slice(bursts, func(i, j int) bool { return bursts[i].AnxTime < bursts[j].AnxTime })
		// Create pools of burst with similar AnxTime, sort by date and find ref and prev burst
		for i, il := 0, 0; i < len(bursts); il = i {
			for ; i < len(bursts) && bursts[i].AnxTime < bursts[il].AnxTime+5; i++ {
			}

			sbursts := bursts[il:i]
			sort.Slice(sbursts, func(j, k int) bool { return sbursts[j].Date.Before(sbursts[k].Date) })
			log.Logger(ctx).Sugar().Debugf("Track %s AnxTime: %d => ref date: %s", track, sbursts[0].AnxTime, sbursts[0].Date)
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

	return len(burstsPerTrackSwath)
}

// burstsFromAnnotations loads bursts features (anxtime, swath and geometry) from annotation files
func burstsFromAnnotations(ctx context.Context, scene *entities.Scene, annotationsProviders []catalog.AnnotationsProvider) ([]*entities.Tile, error) {
	var err, e error
	var annotationsFiles map[string][]byte
	for _, annotationsProvider := range annotationsProviders {
		annotationsFiles, e = annotationsProvider.AnnotationsFiles(ctx, &scene.Scene)
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
		bursts, err := annotations.BurstsFromAnnotation(file, url)
		if err != nil {
			return nil, fmt.Errorf("burstsFromAnnotations.%w", err)
		}
		for anxTime, burst := range bursts {
			if _, ok := anxTimes[anxTime]; !ok {
				anxTimes[anxTime] = struct{}{}

				// Add info from scene
				burstsInventory = append(burstsInventory, &entities.Tile{
					TileLite: entities.TileLite{
						Date:     scene.Data.Date,
						SceneID:  scene.SourceID,
						SourceID: fmt.Sprintf("%s%s_%s_%d", scene.Tags[common.TagOrbitDirection][0:1], scene.Tags[common.TagRelativeOrbit], burst.SwathID, burst.AnxTime),
					},
					Data: common.TileAttrs{
						SwathID: burst.SwathID,
						TileNr:  burst.TileNr,
					},
					AnxTime:     burst.AnxTime,
					GeometryWKT: burst.GeometryWKT,
				})
			}
		}
	}

	return burstsInventory, nil
}
