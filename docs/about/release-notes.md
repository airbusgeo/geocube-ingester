# Release notes

## 1.1.0

### Warning
- catalog/scenes and catalog/tiles: fields are low case

#### Outdated graphs
To fix incoherences or potential limitation in JSON graphs :
- Layer "product" is replaced by "__product__"
- Extension "all" is replaced by "*"
- When importing with extension "zip", the file is not unzipped (use Extension "*" to unzip the file)
- When importing with extension "*", "SAFE" or "dim", the unzip folder is ensured to have the name <layer>.<Extension> (or just <layer> for the former case).

> Be sure to update the processing graphs, otherwise the behaviour will be undefined.

### Functionalities
- Downloader: add a new provider from URL (use --url-provider)

### Bug fixes
- LocalProvider does not require that a date is included in the product name
- GS/FTP: do not try to unzip the downloaded file if the extension is not "zip"

### Optimization
- catalog/scenes catalog/tiles: remove unused or empty fields in the JSON file exported + fields are low case
- Storage: Create zip archive with BestSpeed

## 1.0.3beta

### Functionalities
- AreaToIngest: Add RetryCount to automatically retry a processing or a download RetryCount times
- AreaToIngest: Add IsRetriable to enable or disable the retry of a processing or a download
- AreaToIngest: Add StorageURI to define a custom storage URI for downloader and processor
- Workflow: add endpoint \aoi\{pattern} to list aois by pattern
- ProcessingGraph: add condition=on_failure/on_fatal_failure and error_condition=... to create/index/delete a file in case of failure
- remove CreodiasAnnotationsProvider, add UrlAnnotationsProvider instead of GCSAnnotationBucket
- gs-provider-buckets supports wildcard
- gs-provider downloads the last version if more than one file is present
- rename params Catalog parameter **--gcstorage -> --gcs-annotations-bucket**
- URL patterns support {KEY}-format to be replaced by information extracted from the scene name (see --help)
- Add paging for loadScenes & loadTiles
- Copernicus database catalogue: add **--copernicus-catalog**
- Sobloo, Scihub, Onda, Mundi catalog/provider decomissioning

### Bug fixes
- return EmptyError, in case of ingestion of an empty area
- raise an error if scene.AOI != AOI during an ingestion
- GetDownloadLink from GCSAnnotationsBucket
- PythonLogFilter does not ignore FATAL ERROR anymore
- scihub catalogue: retry 3 times
- oneatlas: exponential retry
- Processor.index: add more time before retry
- ingestScenes might delete records that are used somewhere else
- ingestScenes with more scenes than endDate/startDate
- POST /catalog/aoi without scenes or tiles

### Optimization
- catalog.ScenesToIngest: list all records at once, instead of one by one
- databse: add status to AOI to retrieve the status more efficiently 
- ingestScenes: by batch instead of one by one

