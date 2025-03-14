# Run a payload

The [payload](payload.md) will be used to:
1. List available scenes corresponding to the criterias (Catalogue)
2. Configure the downloading and pre-processing of the scenes (Downloader)
3. Configure the processing of the scenes (Processor)
4. Index the output products (Processor)

## List the available scenes

The first step of the ingestion is to list the scenes available on the AOI at the given dates.
The ingester will query the scenes from the external catalogues configured in the Catalogue Service.

The Catalogue service has the following endpoint that take a payload in input

```shell
curl -F "area=@{payloadFile}" -H {token} {workflow_server}/catalog/scenes
```

> NB: This request supports page/limit parameters to limit the query if the area or the date interval is big : `/catalog/scenes?page={page}&limit={limit}`.
A limit of 1000 scenes is appropriate.

This request returns a geojson file containing a list of features. Each feature is a product and has the following properties:

- `aoi`: name of the AOI, copied from `payload.name`
- `data`: used by the ingester. Some fields (`graph_config`, `graph_name`, `is_retriable`, `storage_uri`) are copied from the `payload`. Others are:
  - `date`: of acquisition of the image
  - `record_id`: id of the record created with `wkt`, `date` and `tags` (ignored at this stage)
  - `metadata`: dictionary of metadata that can be used by the ingester (such as `download_link`)
  - `uuid`: unique id of the image provided by the catalogue
- `id`: unique id given to the scene by the ingester-workflow (ignored at this stage)
- `source_id`: id of the image
- `tags`: tags of the record with which the images will be indexed (dictionary of key:value). The record will be created at the begining of the ingestion
- `wkt`: a WKT of the image extent in EPSG:4326


Example:
```json
{
    "aoi":"DenmarkDemoS2",
    "data":{
        "date":"2022-01-04T10:34:31Z",
        "graph_config":{},
        "graph_name":"CopyProductToStorage",
        "is_retriable":true,
        "record_id":"",
        "storage_uri":"",
        "uuid":"875f96fb-e591-4bcf-8202-fada69733e26"
    },
    "id":0,
    "source_id":"S2A_MSIL1C_20220104T103431_N0510_R108_T32UNG_20240423T092858",
    "tags":{
        "area":"Denmark",
        "cloudCoverPercentage":"50.9439586971957",
        "constellation":"SENTINEL2",
        "ingestionDate":"2022-01-04T10:34:31.000000Z",
        "orbit":"34139",
        "orbitDirection":"",
        "productType":"S2MSI1C",
        "provider":"geocube-ingester",
        "relativeOrbit":"108","satellite":"SENTINEL2A",
        "source":"tutorial","sourceID":"S2A_MSIL1C_20220104T103431_N0510_R108_T32UNG_20240423T092858",
        "uuid":"875f96fb-e591-4bcf-8202-fada69733e26"
    },
    "wkt":"POLYGON ((8.999680177 55.89488809,8.999687664 54.95909887,10.71398549 54.94701782,10.7572757 55.93320247,9.0247385 55.94555573,8.999680177 55.89488809))"
}
```


## List the available tiles

This endpoint can be useful to control how the scenes will be divided into tiles (for Sentinel-1 bursts for example).

```shell
curl -F "area=@{payloadFile}" -H {token} {workflow_server}/catalog/tiles
```

## Start the ingestion (download and processing)

From a list of tiles:
```shell
curl -F "area=@{payloadFile}" -F "tiles=@outputs/tiles.json" -H {token} {workflow_server}/catalog/aoi
```
Example of tiles.json: [here](monitoring.md#tiles)

From a list of scenes:
```shell
curl -F "area=@{payloadFile}" -F "scenes=@outputs/scenes.json" -H {token} {workflow_server}/catalog/aoi
```
Example of scenes.json: [here](monitoring.md#scenes).

From the payload only:
```shell
curl -F "area=@{payloadFile}" -H {token} {workflow_server}/catalog/aoi
```
