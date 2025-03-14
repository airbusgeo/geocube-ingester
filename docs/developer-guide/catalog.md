# Catalogue

NB: This documentation is for developer that want to implement a new Catalogue Provider. For documentation on how to use the catalogue, see [User-Guide/Catalogue](#user-guide/catalog.md).

## Add a new Catalogue

1. For a new constellation/satellite: in file `catalog/catalog.go`, adapt `DoTilesInventory` method in order to be able to interpret the new constellation (otherwise no update needed).
2. In file `catalog/scenes.go`, add newProvider instantiation in `ScenesInventory()` method.
3. For a new constellation/satellite: in file `catalog/entities/entities.go` add the constellation name and adapt `GetConstellation` method in order to manage the new constellation (otherwise no update needed).
4. Add configuration parameters (credentials, endpoint) in `cmd/catalog/main.go`.
5. Implement new catalog in `interface/catalog` following the interface:

```go
type ScenesProvider interface {
	Supports(c common.Constellation) bool
	SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error)
}
```

`SearchScenes` method returns a list of available scenes



