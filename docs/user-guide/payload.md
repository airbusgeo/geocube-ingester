# Payload

The input of the ingester is a payload. It contains an AOI, a date interval, parameters defining the raw products, parameters defining the processing and parameters defining the products to be ingested in the Geocube.

The payload is a GeoJSON (all fields are mandatory unless otherwise stated):

- **AOI** according to GeoJSON standards (type, geometry, coordinates...)
- **name**: Unique name used to identify the Area in the workflow. After a first ingestion, new scenes can be added to the same area, benefiting from automatic scenes reference picking (useful for S1-bursts).
- **start_time**, **end_time**: date interval
- **scene_type**: describing the type of the products to be downloaded
    - **constellation**: Name of the Satellite Constellation (currently supported : sentinel1, sentinel2)
    - **parameters**: (optional) specific parameters to filter the results (see [Catalogue API guide](#catalogue-apis))
- **scene_graph_name**: name of the graph that will be used just after downloading the scene (or "CopyProductToStorage") (see [Processing Graphs](graph.md))
- **tile_graph_name**: name of the graph that will be used to process each tiles (or "Pass") (see [Processing Graphs](graph.md))
- **graph_config**: (optional): specific configuration of the graphs
- **layers**: mapping between layers to be indexed in the Geocube and the corresponding variable.instance from the Geocube (see Geocube Documentation).  
    - **layername**: {"variable":"variable_name", "instance":"instance_name"}
- **record_tags** (optional): user-defined tags for identifying/creating the record in the Geocube.
- **annotations_urls** (optional): list of urls to retrieve Sentinel-1 annotations
- **is_retriable** (optional): define if the processing or download is retriable if a fatal error occurs (or if retry_count is over)
- **retry_count** (optional): define the number of time a processing or download is retried if a transient error occurs
- **storage_uri** (optional): define a custom storage

## Inputs

In ingestion payload, `geometry`, `start_time`, `end_time` and `scene_type` are mandatory in order to list available scenes covering by AOI and between range of time.

### Sentinel1

We need to use `scene_graph_name` in order to extract every bursts from Scenes. This step is processed in downloader as pre-processing (1 job per Scenes).

`tile_graph_name` defines processing to use for every Tiles. This step is processed after downloading every Scenes (processor) (1 job per Tiles).

### Sentinel2, SPOT, PHR

We need to use `scene_graph_name` in order to pre-process data (example: extract Panchromatic & MultiSpectral Image from DIMAP product).

`tile_graph_name` defines processing to use for every Scenes/Tiles. This step is processed after downloading every Scenes (processor).

cf. [Payload ingestion](payload.md#example)

## Catalogue APIS

### Copernicus

Copernicus available parameters: 	`productType`,`platformname`,`polarisationmode`,`sensoroperationalmode`, `cloudcoverpercentage`, `relativeorbitnumber` and `filename`

### OneAtlas

OneAtlas available parameters: 	`productType`,`platform`,`processingLevel`,`cloudCover`, `incidenceAngle`, `workspace` and `relation`

NB: To use other parameters or mix different kind of imagery. You need to group all your images in the same workspace (via OneAtlas) and reference only `workspace` in payload ingestion.

For more information see: [OneAtlas API guide](https://www.geoapi-airbusds.com/api-catalog/oneatlas-data/index.html#tag/Search)

## Utilities

- `Geometry`, `scene_type`, `end_time` and `start_time` will be useful for catalog in order to search corresponding scenes.
- `scene_graph_name` will be useful for pre-processing in downloader (example: extract every burst from scenes) in order to define which graph will be used.
- `tile_graph_name` and `graph_config` will be useful for  processor in order to define which graph will be used.
- `record_tags` and `layers` will be useful in workflow in order to create potential record in Geocube and reference existing variables and instances (for the indexation of the data).

## Endpoints

The Area, with scenes and tiles, is posted to the workflow service that is in charge of creating and running the processing flow.

### Tiles Payload

```shell
curl -F "area=@{payloadFile}" -F "tiles=@outputs/tiles.json" -H {token} {workflow_server}/catalog/aoi
```
Example of tiles.json: [here](monitoring.md#tiles)

### Scenes Payload

```shell
curl -F "area=@{payloadFile}" -F "scenes=@outputs/scenes.json" -H {token} {workflow_server}/catalog/aoi
```
Example of scenes.json: [here](monitoring.md#scenes)


### AOI Payload

```shell
curl -F "area=@{payloadFile}" -H {token} {workflow_server}/catalog/aoi
```


## Example AOI Payload

```json
{
    "name":"DenmarkDemo",
    "type":"Feature",
    "geometry":{
        "type": "MultiPolygon",
        "coordinates":
        [
			[
				[
					[10.061230468750068, 54.88637695312502],
					[9.957128906249977, 54.87246093750002],
					[9.903906250000063, 54.896630859374994],
					[9.80625, 54.90600585937503],
					[9.77119140625004, 55.059912109375034],
					[9.78125, 55.06904296875001],
					[9.830371093750015, 55.05825195312505],
					[9.998828125000045, 54.986474609374994],
					[10.05771484375006, 54.90791015624998],
					[10.061230468750068, 54.88637695312502]
				]
			]
    	]
    },
    "start_time":"2022-01-01T00:00:00.000Z",
    "end_time":"2022-02-10T00:00:00.000Z",
	"scene_type":{
        "constellation":"sentinel1",
        "parameters": {
            "producttype": "SLC",
            "polarisationmode": "VV VH",
            "sensoroperationalmode": "IW",
			"relativeorbitnumber": "44"
        }
    },
    "scene_graph_name":"S1Preprocessing",
    "tile_graph_name":"S1BackscatterCoherence",
    "graph_config":{
        "projection":"EPSG:32632",
        "snap_cpu_parallelism":"8",
        "bkg_resampling": "BISINC_5_POINT_INTERPOLATION"
    },
    "layers":{
		"sigma0_VV": {"variable":"BackscatterSigma0VV", "instance":"RNKell"},
		"sigma0_VH": {"variable":"BackscatterSigma0VH", "instance":"RNKell"},
		"coh_VV": {"variable":"CoherenceVV", "instance":"master"},
		"coh_VH": {"variable":"CoherenceVH", "instance":"master"}
    },
    "record_tags":{
        "source": "tutorial",
		"provider": "geocube-ingester",
		"area":"Denmark"
    }
}
```