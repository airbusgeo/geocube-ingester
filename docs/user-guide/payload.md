# Payload

The input of the ingester is a payload. It contains an AOI, a date interval, parameters defining the raw products, parameters defining the processing and parameters defining the products to be ingested in the Geocube.

The payload will be used to:
1. List available scenes corresponding to the criterias ([Catalogue](catalog.md))
2. Configure the downloading and pre-processing of the scenes (Downloader)
3. Configure the processing of the scenes (Processor)
4. Index the output products (Processor)

The payload is a GeoJSON (all fields are mandatory unless otherwise stated):

- AOI according to GeoJSON standards (`type`, `features`, `geometry`, `coordinates`...)
- `name`: Unique name used to identify the Area in the workflow. After a first ingestion, new scenes can be added to the same area, benefiting from automatic scenes reference picking (useful for S1-bursts).
- `start_time`, `end_time`: date interval

- `scene_type`: describing the type of the products to be downloaded
  - `constellation`: Name of the Satellite Constellation (see [currently supported](catalog.md))
  - `parameters`: (optional) specific parameters to filter the results (see [Catalogue API guide](#catalogue-apis))
- `scene_graph_name`: name of the graph (or "CopyProductToStorage") that will be used just after downloading the scene (see [Processing Graphs](graph.md))
- `tile_graph_name`: name of the graph (or "Pass") that will be used to process each tile (output of the downloader) (see [Processing Graphs](graph.md))
- `graph_config`: (optional): specific configuration of the graphs

- `layers`: mapping between layers to be indexed in the Geocube and the corresponding variable.instance from the Geocube (see Geocube Documentation).  
  - `layername`: {"variable":"variable_name", "instance":"instance_name"}
- `record_tags` (optional): user-defined tags for identifying/creating the record in the Geocube.

- `annotations_urls` (optional): list of urls to retrieve Sentinel-1 annotations
- `is_retriable` (optional): define if the processing or download is retriable if a fatal error occurs (or if retry_count is over)
- `retry_count` (optional): define the number of time a processing or download is retried if a transient error occurs
- `storage_uri` (optional): define a custom storage
- `page`, `limit` (optional): query the n-th `page` (0-based) of the catalog and return `limit` scenes at most.

> Using `page`, `limit`, the number of scenes returned might be less than `limit` **even if** it's not the last page. Check `properties["next"]` for another page (value `true`/`false`)

## Parameters to request the catalogue to find the products

To list available scenes covering an AOI between a range of time, the [Catalogue](catalog.md)) will use the fields `geometry`, `start_time`, `end_time` and `scene_type`.

The `scene_type` defines the `consellation` and the `parameters` depends on the catalogue:

### Copernicus

Available `scene_type/parameters`:

- `platformname`: e.g. `SENTINEL-1`, `SENTINEL-2`
-	`productType`: e.g. `SLC`, `S2MSI1C`
- `filename`

For Sentinel-1 only:

- `polarisationmode`: default `VV VH`
- `sensoroperationalmode`: default `IW`
- `relativeorbitnumber`
  
For optical only:

- `cloudcoverpercentage`: format `[min TO max]`

### OneAtlas

OneAtlas available parameters: 

- `platform`: e.g. `PHR1B`
- `acquisitionIdentifier`: e.g. `ACQ_PNEO4_02426805581362`
- `productType`: e.g. `bundle`
- `processingLevel`
- `cloudCover`: e.g. `[0,10]`
- `incidenceAngle`
- `workspace`
- `relation`

> NB: To use other parameters or mix different kind of imagery. You need to group all your images in the same workspace (via OneAtlas) and reference only `workspace` in payload ingestion.

For more information see: [OneAtlas API guide](https://api.oneatlas.airbus.com/guides/oneatlas-data/g-search/)

## Parameters to download and process the products

- `scene_graph_name` defines a path to a [processing graph](graph.md) that will be used by the downloader to pre-process the products (example: extract every burst from scenes).
- `tile_graph_name` defines a path to a [processing graph](graph.md) that will be used by the processor to process the tiles.
- `graph_config` defines pairs of key-values that can be used by the preceding graphs (see [Graph:Args](graph.md#args))

### Sentinel1

`scene_graph_name` is usually used to extract the bursts from the Scenes. This step is done by the downloader as a pre-processing (1 job per Scene).

`tile_graph_name` defines the processing to do for every Tiles. This step is done by the processor (1 job per Tile).

### Sentinel2, SPOT, PHR

`scene_graph_name` can be used to pre-process data (example: extract Panchromatic & MultiSpectral Image from DIMAP product) but it usually used to copy the data to the ingester storage (`=CopyToStorage`). In this case a Tile is the whole Scene.

`tile_graph_name` defines the processing to do for each Tile. This step is done by the processor.

cf. [Payload ingestion](payload.md#example)

## Parameters to index the output products

- `record_tags` will be used by the catalogue, just before the ingestion starts, to create the records in the Geocube to index the output products
- `layers` will define the match between the output layers of the graph and the variables (and instances) of the Geocube (to index the data).

# Example of an AOI Payload

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
