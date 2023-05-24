# Local installation

## Environment of development
|   Name     | Version  |     link                                          |
|:----------:|:--------:|:-------------------------------------------------:|
|   Golang   | >= 1.16  |      https://golang.org/doc/install               |
|    GDAL    |  >= 3.2  |             https://gdal.org                      |
|   Python   |  >= 3.7  |    https://www.python.org/downloads/              |
| PostgreSQL |  >= 11   |   https://www.postgresql.org/download/            |
|   Docker   |    NC    | https://docs.docker.com/engine/install/           |
|  ESA SNAP  |  >=8.0   | https://step.esa.int/main/download/snap-download/ |


## Build and run Go application

### Messaging broker

#### PGQueue

To use this messaging broker, create the `pgq_jobs` table in your postgres database using the following script `vendor/github.com/airbusgeo/geocube/interface/messaging/pgqueue/create_table.sql`.

```bash
$ psql -h <database_host> -d <database_name> -f vendor/github.com/airbusgeo/geocube/interface/messaging/pgqueue/create_table.sql
```

Then, start the services the following arguments:
- `--pgq-connection`: connection uri to the postgres database (e.g. `postgresql://user:password@localhost:5432/geocube`)
- `--event-queue events`
- `--downloader-queue/--job-queue downloader`
- `--processor-queue/--job-queue processor`

#### PubSub Emulator

A Pub/Sub emulator is available to use PubSub in a local system (with limited capacities).

Please follow the [documentation](https://cloud.google.com/pubsub/docs/emulator) to start the emulator.

Example:
```bash
gcloud beta emulators pubsub start --project=geocube-emulator --host-port $PUBSUB_EMULATOR_HOST
```

After starting pubsub emulator server, the following script creates ingester topics and subscriptions: `tools/pubsub_emulator/main.go`.

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

Example:

```bash
$ export GRAPHPATH=/home/user/geocube-ingester/graph
$ export SNAPPATH=/usr/local/snap/bin/gpt
$ export WORKING_DIR=/home/user/geocube-ingester/data/
$ ./downloader --ps-project geocube-emulator --job-queue ingester-downloader --event-queue ingester-events --local-path $WORKING_DIR/data --storage-uri $WORKING_DIR/output --workdir $WORKING_DIR/tmp -gs-provider-buckets=Sentinel2:gs://gcp-public-data-sentinel-2/tiles/{LATITUDE_BAND}/{GRID_SQUARE}/{GRANULE_ID}/{SCENE}.SAFE --scihub-username=$SCIHUB_USERNAME --scihub-password=$SCIHUB_PASSWORD
```

For more information concerning flags and downloader argument, you can run:

```bash
$ ./downloader --help
Usage of ./downloader:
  -asf-token string
    	ASF token (optional). To configure Alaska Satellite Facility as a potential image Provider.
  -creodias-password string
    	creodias account password (optional)
  -creodias-username string
    	creodias account username (optional). To configure Creodias as a potential image Provider.
  -docker-envs string
    	docker variable env key white list (comma sep) 
  -docker-mount-volumes string
    	list of volumes to mount on the docker (comma separated)
  -docker-registry-password string
    	password to authentication on private registry
  -docker-registry-server string
    	address of server to authenticate on private registry (default "https://eu.gcr.io")
  -docker-registry-username string
    	username to authentication on private registry (default "_json_key")
  -event-queue string
    	name of the queue for job events (pgqueue or pubsub topic)
  -gs-provider-buckets string
    	Google Storage buckets. List of constellation:bucket comma-separated (optional). To configure GS as a potential image Provider.
    		bucket can contain several {IDENTIFIER} than will be replaced according to the sceneName.
    		IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)
    		 
  -job-queue string
    	name of the queue for downloader jobs (pgqueue or pubsub subscription)
  -local-path string
    	local path where images are stored (optional). To configure a local path as a potential image Provider.
  -mundi-seeed-token string
    	mundi seeed-token (optional). To configure Mundi as a potential image Provider.
  -onda-password string
    	onda account password (optional)
  -onda-username string
    	onda account username (optional). To configure ONDA as a potential image Provider.
  -oneatlas-apikey string
    	oneatlas apikey to use
  -oneatlas-auth-endpoint string
    	oneatlas order endpoint to use (default "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token")
  -oneatlas-download-endpoint string
    	oneatlas download endpoint to use (default "https://access.foundation.api.oneatlas.airbus.com/api/v1/items")
  -oneatlas-order-endpoint string
    	oneatlas order endpoint to use (default "https://data.api.oneatlas.airbus.com")
  -oneatlas-username string
    	oneatlas account username (optional). To configure Oneatlas as a potential image Provider.
  -peps-password string
    	peps account password (optional)
  -peps-username string
    	peps account username (optional). To configure PEPS as a potential image Provider.
  -pgq-connection string
    	enable pgq messaging system with a connection to the database
  -ps-project string
    	pubsub subscription project (gcp only/not required in local usage)
  -scihub-password string
    	scihub account password (optional)
  -scihub-username string
    	scihub account username (optional). To configure Scihub as a potential image Provider.
  -storage-uri string
    	storage uri (currently supported: local, gs). To store outputs of the scene preprocessing graph.
  -with-docker-engine
    	activate the support of graph.engine == 'docker' (require a running docker-daemon)
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
$ export GRAPHPATH=./graph
$ export SNAPPATH=<path to ESA SNAP gpt binary>
$ ./processor -flag value
```

Example:

```bash
$ export GRAPHPATH=./graph
$ export SNAPPATH=/usr/local/snap/bin/gpt
$ export WORKING_DIR=/home/user/geocube-ingester/data/
$ ./processor --ps-project geocube-emulator --job-queue ingester-processor --event-queue ingester-events --geocube-server $GEOCUBE_SERVER --geocube-insecure --storage-uri $WORKING_DIR/output --workdir $WORKING_DIR/tmp
```

For more information concerning flags and downloader argument, you can run:

```bash
$ ./processor --help
Usage of ./processor:
  -docker-envs string
    	docker variable env key white list (comma sep) 
  -docker-mount-volumes string
    	list of volumes to mount on the docker (comma separated)
  -docker-registry-password string
    	password to authentication on private registry
  -docker-registry-server string
    	address of server to authenticate on private registry (default "https://eu.gcr.io")
  -docker-registry-username string
    	username to authentication on private registry (default "_json_key")
  -event-queue string
    	name of the queue for job events (pgqueue or pubsub topic)
  -geocube-apikey string
    	geocube server api key
  -geocube-insecure
    	connection to geocube server is insecure
  -geocube-server string
    	address of geocube server (default "127.0.0.1:8080")
  -job-queue string
    	name of the queue for processor jobs (pgqueue or pubsub subscription)
  -pgq-connection string
    	enable pgq messaging system with a connection to the database
  -ps-project string
    	pubsub subscription project (gcp only/not required in local usage)
  -storage-uri string
    	storage uri (currently supported: local, gs). To get outputs of the scene preprocessing graph and store outputs of the tile processing graph.
  -with-docker-engine
    	activate the support of graph.engine == 'docker' (require a running docker-daemon)
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

Example:

```bash
$ export DB_CONNECTION=postgresql://user:password@localhost:5432/ingester?binary_parameters=yes
$ export WORKFLOW_PORT=8082
$ ./workflow --ps-project geocube-emulator --event-queue ingester-events --downloader-queue ingester-downloader --processor-queue ingester-processor --db-connection=$DB_CONNECTION --port $WORKFLOW_PORT --geocube-server $GEOCUBE_SERVER --geocube-insecure --scihub-username "$SCIHUB_USERNAME" --scihub-password "$SCIHUB_PASSWORD"
```

For more information concerning flags and downloader argument, you can run:

```bash
$ ./workflow --help
Usage of ./workflow:
  -bearer-auth string
    	bearer authentication (token) (optional)
  -db-connection string
    	database connection
  -downloader-queue string
    	name of the queue for downloader jobs (pgqueue or pubsub topic)
  -downloader-rc string
    	image-downloader replication controller name (autoscaler)
  -event-queue string
    	name of the queue for job events (pgqueue or pubsub subscription)
  -gcstorage string
    	GCS url where scenes are stored (for annotations) (optional)
  -geocube-apikey string
    	geocube server api key
  -geocube-insecure
    	connection to geocube server is insecure
  -geocube-server string
    	address of geocube server (default "127.0.0.1:8080")
  -max-downloader int
    	Max downloader instances (autoscaler) (default 10)
  -max-processor int
    	Max Processor instances (autoscaler) (default 900)
  -namespace string
    	namespace (autoscaler)
  -oneatlas-apikey string
    	oneatlas account apikey (to generate an api key for your account: https://account.foundation.oneatlas.airbus.com/api-keys)
  -oneatlas-auth-endpoint string
    	oneatlas order endpoint to use (default "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token")
  -oneatlas-endpoint string
    	oneatlas endpoint to search products from the catalogue (default "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch")
  -oneatlas-order-endpoint string
    	oneatlas order endpoint to estimate processing price (default "https://data.api.oneatlas.airbus.com")
  -oneatlas-username string
    	oneatlas account username (optional). To configure Oneatlas as a potential image Provider.
  -pgq-connection string
    	enable pgq messaging system with a connection to the database
  -port string
    	workflow port ot use (default "8080")
  -processor-queue string
    	name of the queue for processor jobs (pgqueue or pubsub topic)
  -processor-rc string
    	tile-processor replication controller name (autoscaler)
  -ps-project string
    	pubsub subscription project (gcp only/not required in local usage)
  -scihub-password string
    	password to connect to the Scihub catalog service
  -scihub-username string
    	username to connect to the Scihub catalog service
  -tls
    	enable TLS protocol (certificate and key must be /tls/tls.crt and /tls/tls.key)
```
