package catalog

import (
	"context"

	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/paulsmith/gogeos/geos"
)

type AnnotationsProvider interface {
	AnnotationsFiles(ctx context.Context, scene *common.Scene) (map[string][]byte, error)
}

type ScenesProvider interface {
	SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error)
}
