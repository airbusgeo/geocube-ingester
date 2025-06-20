# Catalogue

NB: This documentation is for developer that want to implement a new Catalogue Provider. For documentation on how to use the catalogue, see [User-Guide/Catalogue](#user-guide/catalog.md).

## Add a new Catalogue

1. In file `catalog/scenes.go`, add newProvider instantiation in `ScenesInventory()` method.
2. For a new constellation/satellite: in file `catalog/common/naming.go` add the constellation name and modify `GetConstellationFromString` method.
3. Add configuration parameters (credentials, endpoint) in `cmd/catalog/main.go` and  `cmd/workflow/main.go`.
4. Implement new catalog in `interface/catalog` following the interface:

```go
type ScenesProvider interface {
	Supports(c common.Constellation) bool
	SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) (entities.Scenes, error)
}
```

`SearchScenes` method returns a list of available scenes

5. Update the documentation:
   1. [docs/user-guide/catalog.md](../user-guide/catalog.md) to describe the new catalogue and explain how to configure it.
   2. [docs/user-guide/payload.md](../user-guide/payload.md) to describe the specific parameters to set in the payload-file to request this catalogue.



