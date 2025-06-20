# Interfaces

To integrate into the environment of deployment, the ingester has an interface layer. Some implementations of this layer are available and the user is free to implement others depending on its own environment.

## Messaging

The messaging interface is available here : `vendor/github.com/geocube/interface/messaging/`.

It is used to communicate between `workflow`, `downloader` and `processor` service. It is configured in the corresponding `main.go` in the `cmd` folder.

### Postgres-based implementation

A messaging interface based on postgres is implemented using the [btubbs/pgq](https://github.com/btubbs/pgq) library: `vendor/github.com/geocube/interface/messaging/pgqueue`. This implementation has autoscaling capabilities.

>>The postgreqsl database must be configured to accept as many connections as the number of workers, because pgqueue keeps the connection open during the whole processing.


### PubSub implementation

Ingester supports PubSub (Google Cloud Platform) messaging broker : `vendor/github.com/geocube/interface/messaging/pubsub`.

Three topics/subscriptions must be created:

- To communicate from workflow to downloader service i.e : ingester-downloader
- To communicate from workflow to processor service i.e : ingester-processor
- To communicate from downloader & processor to workflow i.e : ingester-event

> NB: Topics & Subscriptions must be created before running Downloader, Processor and Workflow.

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

The Downloader service is currently able to download products from different provider listed [here](../user-guide/providers.md).

Depending on the provider, the user may need credentials. Please refer to the API documentation of the provider.

### Add a new provider

See [Developer Guide/Provider](providers.md).

## Image Catalog

To be able to list the scenes available over an AOI, the Ingester has an interface to an external catalogue service : `interface/catalog/catalog.go`

### Current implementations

The Ingester is currently able to connect to the catalogues defined [here](../user-guide/catalog.md)


### Add a new catalogue

See [Developer Guide/Provider](catalog.md).
