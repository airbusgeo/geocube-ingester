# Release notes

## 1.0.3beta

### Functionalities
- ProcessingGraph: add condition=on_failure/on_fatal_failure and error_condition=... to create/index/delete a file in case of failure
- remove CreodiasAnnotationsProvider, add UrlAnnotationsProvider instead of GCSAnnotationBucket
- gs-provider-buckets supports wildcard
- rename params Catalog parameter --gcstorage -> --gcs-annotations-bucket
- URL patterns support {KEY}-format to be replaced by information extracted from the scene name (see --help)
- Add paging for loadScenes & loadTiles
- Onda catalogue (/!\ seems to not return all the S2 scenes !!) and Creodias Catalogue (does not support filename option)
- Sobloo catalog/provider decomissioning

### Bug fixes
- return EmptyError, in case of ingestion of an empty area
- raise an error if scene.AOI != AOI during an ingestion
- GetDownloadLink from GCSAnnotationsBucket
- PythonLogFilter does not ignore FATAL ERROR anymore
- scihub catalogue: retry 3 times
- oneatlas: exponential retry

### Optimization
- catalog.ScenesToIngest: list all records at once, instead of one by one

