# Provider

Providers are implemented in order to download scenes. They are called one by one until the corresponding image is found.

- [Scihub](providers.md#scihub): sentinel1 & 2 scenes
- [Creodias](providers.md#creodias): for retrieving the annotations of Sentinel1 products
- [GCS](providers.md#gcs): for retrieving the annotations of an Archive stored in GCS
- [Local](providers.md#local-directory): directory sentinel1 & 2 scenes
- [Mundi](providers.md#mundi): sentinel1 & 2 scenes
- [Onda](providers.md#onda): sentinel1 & 2 scenes
- [OneAtlas](providers.md#oneatlas): PHR & SPOT scenes
- [Peps](providers.md#peps): sentinel1 & 2 scenes
- [ASF](providers.md#asf): sentinel1 & 2 scenes

The scenes to be downloaded are sent to the Downloader Service, then the tiles to be processed are sent to the Processor Service.

If an autoscaler is configured, the downloading and the processing are done in parallel using all available machines.
## Creodias

Creodias account credentials are needed.

`creodias-username` and `creodias-password` workflow and downloader arguments must be defined.

`https://finder.creodias.eu/resto/api/collections/<constellation>/search.json` endpoint is use to request Creodias.

and `https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token` in order to get JWT Token.

Scihub is requested with sceneName parameter.

For more information see: [Creodias Documentation](https://creodias.eu/sentinel-hub-documentation)

## GCS

The downloader service must have the rights to read files on buckets.

`gs-provider-buckets` workflow and downloader arguments must be defined.

List of constellation:bucket comma-separated. 

bucket can contain several {IDENTIFIER} than will be replaced according to the sceneName. 

IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)

For more information see: [GCS Documentation](https://cloud.google.com/storage)

## Local directory

No need credentials. 

`local-path` downloader argument must be defined (local path where images are stored)

## Mundi

`mundi-seeed-token` downloader argument must be defined.

Mundi uses `https://mundiwebservices.com/dp/s1-l%s-%s-%04d-q%d/%04d/%02d/%02d/%s/%s/%s.zip` for Sentinel1 and `https://mundiwebservices.com/dp/s2-%s-%04d-q%d/%s/%s/%s/%04d/%02d/%02d/%s.zip` for Sentinel2.

For more information see: [Mundi Documentation](https://mundiwebservices.com/help/documentation)

## Onda

Onda account credentials are needed.

`onda-username` and `onda-password` downloader arguments must be defined.

Onda uses `https://catalogue.onda-dias.eu/dias-catalogue/Products(%s)/$value` endpoint.

For more information see: [Onda Documentation](https://www.onda-dias.eu/cms/knowledge-base/)

## OneAtlas

OneAtlas account credentials are needed.

`oneatlas-username` and `oneatlas-apikey` workflow and downloader arguments must be defined.

By default, OneAtlas uses: 

- `https://access.foundation.api.oneatlas.airbus.com/api/v1/items` as download endpoint  
- `https://data.api.oneatlas.airbus.com` as order endpoint
- `https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token` as authentication endpoint

It's possible to use another endpoint by defining `oneatlas-download-endpoint`, `oneatlas-order-endpoint` and `oneatlas-auth-endpoint`.


Important: concerning pricing, OneAtlas provider will be process and download image while credits are available. 

For more information see: [OneAtlas Documentation](https://oneatlas.airbus.com/home)

## Peps

Peps account credentials are needed.

`peps-username` and `peps-password` downloader arguments must be defined.

Peps uses `https://peps.cnes.fr/resto/api/collections/search.json?` endpoint.

For more information see: [Peps Documentation](https://peps.cnes.fr/rocket/#/home)


## ASF

Asf account credentials are needed.

`asf-token` downloader argument must be defined.

Asf uses `https://datapool.asf.alaska.edu/SLC/S{MISSION_VERSION}/{SCENE}.zip` for SLC product and `https://datapool.asf.alaska.edu/GRD-HD/S{MISSION_VERSION}/{SCENE}.zip` for GRD products.

[ASF Documentation](https://asf.alaska.edu/data-sets/sar-data-sets/sentinel-1/sentinel-1-documents-tools/)
 




