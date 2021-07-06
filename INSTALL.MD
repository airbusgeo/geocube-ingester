# Ingester Installation Guide

## Table of Contents

- [Presentation](#presentation)
- [Prerequisites](#prerequisites)
  - [Developpement Environment](#developpement-environment)
- [Interfaces](#interfaces)
  - [Storage](#storage)
    - [Interface](#interface)
    - [Currently supported storages](#currently-supported-storages)
  - [Messaging](#messaging)
    - [Interface](#interface-1)
    - [PubSub implementation](#pubsub-implementation)
  - [Database](#database)
    - [Interface](#interface-2)
    - [PostgreSQL implementation](#postgresql-implementation)
  - [Geocube](#geocube)
  - [Image Provider](#image-provider)
    - [Interface](#interface-3)
    - [Current Implementations](#current-implementations)
  - [Image Catalog](#image-catalog)
    - [Interface](#interface-4)
    - [Current implementations](#current-implementations-1)
- [Installation - local environment](#installation---local-environment)
  - [Build and run Go application](#build-and-run-go-application)
    - [Messaging broker](#messaging-broker)
      - [PubSub Emulator](#pubsub-emulator)
    - [Downloader](#downloader)
    - [Processor](#processor)
    - [Workflow](#workflow)
  - [Run with Docker](#run-with-docker)
- [Installation - Kubernetes Cluster](#installation---kubernetes-cluster)
  - [Prerequisites](#prerequisites-1)
    - [Container registry](#container-registry)
    - [Create Namespace](#create-namespace)
  - [Create Secrets in namespace](#create-secrets-in-namespace)
  - [Create deployment in namespace](#create-deployment-in-namespace)

# Presentation

The Geocube Ingester is an example of a complete and parallelizable service to feed the Geocube. The user posts an AOI, a time interval and a set of parameters (to compute the output layers). The ingester takes care of everything from the downloading of the products, the computing and its parallelization and the ingestion in the Geocube.

It is composed of three services : workflow, downloader and processor. It is connected to a Geocube and has a couple of interfaces to integrate in the user environment. Some implementations of the interfaces are available and the user is free to implement others according to its environment.

![Ingester Architecture](./Jupyter/data/IngesterArchitecture.png)

# Prerequisites

The ingester needs:
- a relational database (currently supported Postgresql >= 11)
- a messaging System to exchange messages between services (currently supported: Pub/Sub)
- an Object Storage, readable and writable (currently supported: local storage or GCS)
- a catalogue service, to get the list of products available over an AOI (currently supported: Scihub for Sentinel 1 and 2 with an appropriate user account)
- an image provider, to download the products returned by the catalogue service (currently supported : Sobloo, PEPS, Creodias, Onda, Mundis, GCS, local provider for Sentinel 1 and 2 with an appropriate user account)

## Developpement Environment
|   Name     | Version  |     link                                          |
|:----------:|:--------:|:-------------------------------------------------:|
|   Golang   | >= 1.16  |      https://golang.org/doc/install               |
|    GDAL    |  >= 3.2  |             https://gdal.org                      |
|   Python   |  >= 3.7  |    https://www.python.org/downloads/              |
| PostgreSQL |  >= 11   |   https://www.postgresql.org/download/            |
|   Docker   |    NC    | https://docs.docker.com/engine/install/           |
|  ESA SNAP  |  >=8.0   | https://step.esa.int/main/download/snap-download/ |

# Interfaces 

To integrate into the environment of deployment, the ingester has an interface layer. Some implementations of this layer are available and the user is free to implement others depending on its own environment.

## Storage

### Interface

The storage is used to download products and to store intermediate images and the final images that are indexed in the Geocube. It must be accessible in reading and writing.

The interface is available in `vendor/github.com/geocube/interface/storage` package.
Please, refer to the Geocube Installation Guide to implement another interface.


### Currently supported storages

The ingester supports two storage systems: GCS and filesystem.

## Messaging

### Interface

The messaging interface is available here : `vendor/github.com/geocube/interface/messaging/`.
It is used to communicate between `workflow`, `downloader` and `processor` service. It is configured in the corresponding `main.go` in the `cmd` folder.

### PubSub implementation

Ingester supports PubSub (Google Cloud Platform) messaging broker : `vendor/github.com/geocube/interface/messaging/pubsub`.

Three topics/subscriptions must be created:
- To communicate from workflow to downloader service i.e : ingester-downloader
- To communicate from workflow to processor service i.e : ingester-processor
- To communicate from downloader & processor to workflow i.e : ingester-event

NB: Topics & Subscriptions must be created before running Downloader, Processor and Workflow.

A Pub/Sub emulator is available to use PubSub in a local system (with limited capacities).

Please follow the [documentation](https://cloud.google.com/pubsub/docs/emulator) to install and start the emulator.

## Database
### Interface

The database interface is available here : `interface/database/db.go`.
It is used by the Workflow as a parameter of the service and it is configured in the following file: `cmd/workflow/main.go`.

### PostgreSQL implementation

Ingester currently supports a Postgresql database : `interface/database/pg/`
Create a database and run the installation SQL script in order to create all tables, schemas and roles.
This script is available in Geocube code source in `interface/database/pg/db.sql`

```bash
$ psql -h <database_host> -d <database_name> -f interface/database/pg/db.sql
```

## Geocube

Workflow and Processor components must be connected to Geocube. You need to install Geocube before installing Ingester.
From a Geocube point of view, the Ingester will create new records and index the processed data.
The Ingester will connect to the Geocube using the GoLang-Client library : `vendor/github.com/geocube/client/go`.

## Image Provider

### Interface

To download images from data-storages, the ingester has the current interface:
`interface/provider/provider.go`. It is used by the Downloader service and configured in `cmd/downloader/main.go`.

### Current Implementations

The Downloader service is currently able to download Sentinel 1 & 2 products from:
- Sobloo
- Peps
- Scihub
- Onda
- Creodias
- Mundi
- Local file system
- GCS

Depending on the implementation, the user may need to create an account. Please refer to the documentation of the provider.

## Image Catalog

### Interface

To be able to list the scenes available over an AOI, the Ingester has an interface to an external catalog service : `interface/catalog/catalog.go`

### Current implementations

The Ingester is currently able to connect to the following catalogs:
- Scihub for retrieving the sentinel1 & 2 scenes
- Creodias for retrieving the annotations of Sentinel1 products
- GCS for retrieving the annotations of an Archive stored in GCS


# Installation - local environment
## Build and run Go application

### Messaging broker

#### PubSub Emulator

A Pub/Sub emulator is available to use PubSub in a local system (with limited capacities).

Please follow the [documentation](https://cloud.google.com/pubsub/docs/emulator) to install and start the emulator.

After starting pubsub emulator server (make sure that it's running on port 8085), the following script creates ingester topics and subscriptions: `tools/pubsub_emulator/main.go`.

```bash
$ go run tools/pubsub_emulator/main.go --project geocube-emulator
2021/06/16 14:26:06 New client for project geocube-emulator
2021/06/16 14:26:06 Create Topic : ingester-downloader
2021/06/16 14:26:06 Create Topic : ingester-processor
2021/06/16 14:26:06 Create Topic : ingester-events
2021/06/16 14:26:06 Create Subscription : ingester-downloader
2021/06/16 14:26:06 Create Subscription : ingester-processor
2021/06/16 14:26:06 Create Subscription : ingester-events
2021/06/16 14:26:06 Done!
```

In order to run the ingester with the PubSub emulator, you must define the `PUBSUB_EMULATOR_HOST` environment variable (by default `localhost:8085`) **before** starting services.

### Downloader

You can find the downloader main application in `cmd/downloader` folder.
Build application:

```bash
$ go build
$ ls -l
-rwxrwxr-x 1 user user 17063376 juin  11 15:45 downloader
-rw-rw-r-- 1 user user    11214 juin   9 16:09 main.go
```

Downloader needs the path of the local install of ESA SNAP and the `graph` folder (at the root of geocube-ingester).

Run application:

```bash
$ export GRAPHPATH=<geocube-ingester>/graph
$ export SNAPPATH=<path to ESA SNAP gpt binary>
$ ./downloader -flag value
```

Example (run):

```bash
$ export GRAPHPATH=/home/user/geocube-ingester/graph
$ export SNAPPATH=/usr/local/snap/bin/gpt
$ ./downloader --project geocube-emulator --psSubscription=ingester-downloader --psEvent=ingester-events --storage-uri=$STORAGE_URI -gs-provider-buckets=Sentinel2:gs://gcp-public-data-sentinel-2/tiles/{UTM}/{LATITUDE_BAND}/{GRID_SQUARE}/{GRANULE_ID}/{SCENE} --scihub-username=$SCIHUB_USERNAME --scihub-password=$SCIHUB_PASSWORD
```

For more information concerning flags and downloader argument, you can run:

```bash
$ ./downloader --help
Usage of ./downloader:
  -creodias-password string
        creodias account password (optional)
  -creodias-username string
        creodias account username (optional)
  -gs-provider-buckets string
        Google Storage buckets. List of constellation:bucket comma-separated (optional).
                bucket can contain several {IDENTIFIER} than will be replaced according to the sceneName.
                IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)

  -local-path string
        local path where images are stored (optional)
  -mundi-seeed-token string
        mundi seeed-token (optional)
  -onda-password string
        onda account password (optional)
  -onda-username string
        onda account username (optional)
  -peps-password string
        peps account password (optional)
  -peps-username string
        peps account username (optional)
  -project string
        subscription project (gcp only/not required in local usage)
  -psEvent string
        pubsub events topic name
  -psSubscription string
        pubsub image subscription name
  -scihub-password string
        scihub account password (optional)
  -scihub-username string
        scihub account username (optional)
  -sobloo-apikey string
        sobloo api-key (optional)
  -storage-uri string
        storage uri (currently supported: local, gs)
  -workdir string
        working directory to store intermediate results (default "/local-ssd")
```

### Processor

You can find the processor main application in `cmd/processor` folder.
Build application:

```bash
$ go build
$ ls -l
-rwxrwxr-x 1 user user 17063376 juin  11 15:45 processor
-rw-rw-r-- 1 user user    11214 juin   9 16:09 main.go
```

Processor needs the path of the local install of ESA SNAP and the `graph` folder (at the root of geocube-ingester).

Run application:

```bash
$ export GRAPHPATH=./data/graph
$ export SNAPPATH=<path to ESA SNAP gpt binary>
$ ./processor -flag value
```

Example running command:

```bash
$ export GRAPHPATH=./data/graph
$ export SNAPPATH=/usr/local/snap/bin/gpt
$ ./processor --project geocube-emulator --psSubscription=ingester-processor --psEvent=ingester-events --storage-uri=$STORAGE_URI --geocube-server=$GEOCUBE_SERVER --geocube-insecure
```

For more information concerning flags and downloader argument, you can run:

```bash
$ ./processor --help
Usage of ./processor:
  -geocube-apikey string
        geocube server api key
  -geocube-insecure
        connection to geocube server is insecure
  -geocube-server string
        address of geocube server
  -project string
        subscription project (gcp only/not required in local usage)
  -psEvent string
        pubsub events topic name
  -psSubscription string
        pubsub tile subscription name
  -storage-uri string
        storage uri (currently supported: local, gs)
  -workdir string
        working directory to store intermediate results (default "/local-ssd")
```

### Workflow

You can find the workflow main application in `cmd/workflow` folder.
Build application:

```bash
$ go build
$ ls -l
-rwxrwxr-x 1 user user 17063376 juin  11 15:45 workflow
-rw-rw-r-- 1 user user    11214 juin   9 16:09 main.go
```

Run application:

```bash
$ ./workflow -flag value
```

Example running command:

```bash
$ ./workflow --project geocube-emulator --psSubscription ingester-events --psDownloader-topic ingester-downloader --psProcessor-topic ingester-processor --dbConnection=postgresql://postgres:password@localhost:5432/ingester?binary_parameters=yes --port $WORKFLOW_PORT --geocube-server $GEOCUBE_SERVER --geocube-insecure
```

For more information concerning flags and downloader argument, you can run:

```bash
$ ./workflow --help
Usage of ./workflow:
  -dbConnection string
        database connection
  -downloader-rc string
        image-downloader replication controller name (autoscaler)
  -gcstorage string
        GCS url where scenes are stored (for annotations) (optional)
  -geocube-apikey string
        geocube server api key
  -geocube-insecure
        connection to geocube server is insecure
  -geocube-server string
        address of geocube server
  -max-downloader int
        Max downloader instances (autoscaler) (default 10)
  -max-processor int
        Max Processor instances (autoscaler) (default 900)
  -namespace string
        namespace (autoscaler)
  -port string
        workflow port ot use (default "8080")
  -processor-rc string
        tile-processor replication controller name (autoscaler)
  -project string
        subscription project (gcp only/not required in local usage)
  -psDownloader-topic string
        pubsub image-downloader topic name
  -psProcessor-topic string
        pubsub tile-processor topic name
  -psSubscription string
        pubsub event subscription name
  -scihub-password string
        password to connect to the Scihub catalog service
  -scihub-username string
        username to connect to the Scihub catalog service
  -sobloo-apikey string
        apikey to connect to the sobloo ordering service (optional)
```

## Run with Docker

All dockerfile are available. You can build docker images:

```bash
$ docker build -f cmd/downloader/Dockerfile -t $DOWNLOADER_IMAGE .
$ docker build -f cmd/processor/Dockerfile -t $PROCESSOR_IMAGE .
$ docker build -f cmd/workflow/Dockerfile -t $WF_IMAGE .
```

You can run `docker run` command in order to start the application.
For more information concerning running option, see: https://docs.docker.com/engine/reference/commandline/run/

# Installation - Kubernetes Cluster
## Prerequisites

Kubctl must be installed and configured in order to be connected to the right kubernetes cluster.
You also need database and Messaging (kubernetes examples are available in Geocube Installation Guide) these same examples can be used here.
Ingester should be deployed in the same cluster as Geocube, in a different namespace.

### Container registry

The Information concerning Container Registry is described in [Geocube Installation Guide/Installation - Kubernetes Cluster/Container Registry](https://github.com/airbusgeo/geocube/INSTALL.MD#container-registry)


### Create Namespace

You must define a CLUSTER variable environment.
The dedicated namespace `ingester` is created by running the following command:

```bash
$ kubectl apply -f deploy/k8s/namespace.yaml
```

## Create Secrets in namespace

Kubernetes configuration file is available here: `deploy/k8s/secrets.yaml`. All the parameters between `{{}}` are mandatory:
1. `{{GEOCUBE_SERVER}}`: uri of the Geocube (eg. `127.0.0.1:8080`)
2. `{{STORAGE_URI}}`: uri where to store the outputs (layers) of the Ingester (eg. `/ingester/` or `gs://ingester/`)
3. `{{DB_CONNECTION}}`: uri of the database (eg. `postgresql://user:password@localhost:5432/geocube`)

The other parameters (mainly authentication information for image providers) are optional.

Ingester server must have sufficient rights in order to read and write into database. For more information, see: https://www.postgresql.org/docs/11/auth-pg-hba-conf.html

After that, secrets can be created:

```bash
$ kubectl apply -f deploy/k8s/public/secrets.yaml
```

## Create deployment in namespace

In order to start Ingester, you have to define some parameters in `deploy/k8s/public/workflow.yaml` (all the parameters between `{{}}` are mandatory):

1. `{{WORKFLOW_IMAGE}}`: Workflow docker image  (eg. `<container_registry>/processor:<tag>`)
3. `{{DOWNLOADER_IMAGE}}`: Downloader docker image  (eg. `<container_registry>/downloader:<tag>`)
4. `{{PROCESSOR_IMAGE}}`: Processor docker image  (eg. `<container_registry>/processor:<tag>`)

Then, deployement can be created in namespace `ingester` by running the following command:

```bash
$ kubectl apply -f deploy/k8s/public/workflow.yaml
```

NB:
- Workflow.yaml kubernetes file is an example of Ingester deployment in the cloud. You need to adapt them in order to configure database, messaging and storage access.
- In you want to use pubsub emulator, you need to add `PUB_SUB_EMULATOR` variable environment in your deployment and replication controller (already describe in Geocube Documentation).

Ex configuration with pubSub emulator:

```kubernetes helm
env:
  - name: PUBSUB_EMULATOR_HOST
    value: 0.0.0.0:8085
```
