# Docker Installation

## Local docker engine

All dockerfile are available. You can build docker images:

```bash
$ docker build -f cmd/downloader/Dockerfile -t $DOWNLOADER_IMAGE .
$ docker build -f cmd/processor/Dockerfile -t $PROCESSOR_IMAGE .
$ docker build -f cmd/workflow/Dockerfile -t $WF_IMAGE .
```

You can run `docker run` command in order to start the application.
For more information concerning running option, see: https://docs.docker.com/engine/reference/commandline/run/

Examples with pgqueue:
```bash
$ export DB_CONNECTION=postgresql://user:password@localhost:5432/ingester?binary_parameters=yes
$ export WORKFLOW_PORT=8082
$ export STORAGE=/ingester-storage

$ docker run --rm --network=host $WF_IMAGE --pgq-connection=$DB_CONNECTION --event-queue ingester-events --downloader-queue ingester-downloader --processor-queue ingester-processor --db-connection=$DB_CONNECTION --port $WORKFLOW_PORT --geocube-server $GEOCUBE_SERVER --scihub-username "$SCIHUB_USERNAME" --scihub-password "$SCIHUB_PASSWORD"

$ docker run --rm --network=host -v $STORAGE:$STORAGE $DOWNLOADER_IMAGE --pgq-connection=$DB_CONNECTION  --job-queue ingester-downloader --event-queue ingester-events --local-path $STORAGE/products --storage-uri $STORAGE --workdir /tmp -gs-provider-buckets=Sentinel2:gs://gcp-public-data-sentinel-2/tiles/{LATITUDE_BAND}/{GRID_SQUARE}/{GRANULE_ID}/{SCENE}.SAFE --scihub-username=$SCIHUB_USERNAME --scihub-password=$SCIHUB_PASSWORD

$ docker run --rm --network=host -v $STORAGE:$STORAGE $PROCESSOR_IMAGE --pgq-connection=$DB_CONNECTION  --job-queue ingester-processor --event-queue ingester-events --geocube-server $GEOCUBE_SERVER --storage-uri $STORAGE --workdir /tmp
```

## Docker compose

A docker-compose file is provided as example. It's a minimal example, so feel free to edit it to take advantage of the full power of the Geocube-Ingester.

- Copy the `./cmd/dockerfiles/.env.example` to `./cmd/dockerfiles/.env`
- Edit `./docker/.env` to set the env variables.
- Build the base image
- `cd cmd/dockerfiles` and `docker-compose up`