# Interfaces

To integrate into the environment of deployment, the ingester has an interface layer. Some implementations of this layer are available and the user is free to implement others depending on its own environment.

## Messaging

The messaging interface is available here : `vendor/github.com/geocube/interface/messaging/`.

It is used to communicate between `workflow`, `downloader` and `processor` service. It is configured in the corresponding `main.go` in the `cmd` folder.

### Pgqueue implementation

A messaging interface based on postgres is implemented using the [btubbs/pgq](https://github.com/btubbs/pgq) library: `vendor/github.com/geocube/interface/messaging/pgqueue`. This implementation has autoscaling capabilities.

### PubSub implementation

Ingester supports PubSub (Google Cloud Platform) messaging broker : `vendor/github.com/geocube/interface/messaging/pubsub`.

Three topics/subscriptions must be created:

- To communicate from workflow to downloader service i.e : ingester-downloader
- To communicate from workflow to processor service i.e : ingester-processor
- To communicate from downloader & processor to workflow i.e : ingester-event

NB: Topics & Subscriptions must be created before running Downloader, Processor and Workflow.

A Pub/Sub emulator is available to use PubSub in a local system (with limited capacities).

Please follow the [documentation](https://cloud.google.com/pubsub/docs/emulator) to install and start the emulator.


## Storage

The storage is used to download products and to store intermediate images and the final images that are indexed in the Geocube. It must be accessible in reading and writing.

The interface is available in `vendor/github.com/geocube/interface/storage` package.
Please, refer to the Geocube Installation Guide to implement another interface.

### Currently supported storages

The ingester supports two storage systems: GCS and filesystem.

## Database 

The database interface is available here : `interface/database/db.go`.
It is used by the Workflow as a parameter of the service and it is configured in the following file: `cmd/workflow/main.go`.

### PostgreSQL implementation

Ingester currently supports a Postgresql database : `interface/database/pg/`
Create a database and run the installation SQL script in order to create all tables, schemas and roles.
This script is available in Geocube code source in `interface/database/pg/db.sql`

```bash
$ psql -h <database_host> -d <database_name> -f interface/database/pg/db.sql
```

## Image Provider

To download images from data-storages, the ingester has the current interface:
`interface/provider/provider.go`. It is used by the Downloader service and configured in `cmd/downloader/main.go`.

### Current Implementations

The Downloader service is currently able to download Sentinel 1 & 2 products from:

- Peps
- Copernicus (wip)
- Creodias
- Local file system
- GCS
- Alaska Satellite Facility

Depending on the provider, the user may need credentials. Please refer to the API documentation of the provider.

### Add a new provider

1. Add configuration parameters (credentials, endpoint) in `cmd/downloader/main.go` and add the new provider to the list of providers.
2. Implement the new provider in `ìnterface/provider` with methods:

```go
Name() string
Download(ctx context.Context, scene common.Scene, localDir string) error
``` 

## Image Catalog

To be able to list the scenes available over an AOI, the Ingester has an interface to an external catalogue service : `interface/catalog/catalog.go`

### Current implementations

The Ingester is currently able to connect to the following catalogues:

- Copernicus for sentinel1 & 2 scenes
- GCS for bursts annotations of an archive stored in GCS


<div id="custom-interface"></div>
### Add a new catalogue

In order to add a new catalogue:

1. For a new constellation/satellite: in file `catalog/catalog.go`, adapt `DoTilesInventory` method in order to be able to interpret the new constellation (otherwise no update needed).
2. In file `catalog/scenes.go`, add newProvider instantiation in `ScenesInventory` method.
3. For a new constellation/satellite: in file `catalog/entities/entities.go` add the constellation name and adapt `GetConstellation` method in order to manage the new constellation (otherwise no update needed).
4. Add configuration parameters (credentials, endpoint) in `cmd/catalog/main.go`.
5. Implement new catalog in `interface/catalog` with method:

```go
SearchScenes(ctx context.Context, area *entities.AreaToIngest, aoi geos.Geometry) ([]*entities.Scene, error)
```

This method returns a list of available scenes



