# Provider

> NB: This documentation is for user that want to use the providers. For documentation on how to implement a new provider, see [Developer-Guide/Providers](#developer-guide/provideres.md).

Providers are implemented in order to download scenes. They are called one by one until the corresponding image is found.

- [Copernicus](providers.md#copernicus): Sentinel scenes
- [Creodias](providers.md#creodias): Sentinel scenes
- [GCS](providers.md#gcs): any scenes stored in GCS (can be used to retrieve the annotations of an Sentinel1 archive stored in GCS)
- [Local](providers.md#local-directory): any scenes stored locally
- [OneAtlas](providers.md#oneatlas): Airbus scenes (SPOT, Pleiades, PNEO)
- [ASF](providers.md#asf): sentinel1 & 2 scenes
- [Landsat AWS](providers.md#landsat-aws): Landsat 8&9
- [USGS M2M](providers.md#usgs-m2m): Landsat scenes

The scenes to be downloaded are sent to the Downloader Service, then the tiles to be processed are sent to the Processor Service.

If an autoscaler is configured, the downloading and the processing are done in parallel using all available machines.

## Creodias

Creodias account credentials are needed.

`creodias-username`, `creodias-password`, and `creodias-totp-secret` workflow and downloader arguments must be defined.

> **Note**: Since Creodias now requires 2FA (Two-Factor Authentication), you must provide a TOTP secret (32-character base32 string). See [How to get your TOTP secret](#how-to-get-your-totp-secret).

An optional `creodias-cache-uri` argument can be specified to cache the authentication token (e.g., `/tmp/creodias_cache` or `gs://bucket/cache`). This is useful to avoid re-authenticating on every download request. The token is automatically cached in a file named `creodias_token.cache` in the specified URI.

`https://finder.creodias.eu/resto/api/collections/<constellation>/search.json` endpoint is use to request Creodias.

and `https://identity.cloudferro.com/auth/realms/Creodias-new/protocol/openid-connect/token` in order to get JWT Token.

For more information see: [Creodias Documentation](https://creodias.eu/sentinel-hub-documentation)

### How to get your TOTP secret

1. During account setup at https://horizon.cloudferro.com/, when prompted for 2FA setup:
   - Click **"Unable to scan?"** to reveal the secret in plain text
   - Copy and save this 32-character code securely
   - Complete the 2FA setup

2. If you already have an account with 2FA enabled:
   - Use KeePassXC or similar to extract the secret from your TOTP app
   - Or reset your TOTP secret via the Creodias account settings

3. For more details, see: [Creodias 2FA Documentation](https://creodias.docs.cloudferro.com/en/latest/eodata/Use-Bash-to-automate-generating-API-tokens-for-accessing-and-downloading-EODATA-when-2FA-is-enabled-on-Creodias.html)

## Copernicus

Copernicus account credentials are needed.

`copernicus-username` and `copernicus-password` downloader arguments must be defined.

For more information see: [Copernicus Documentation](https://documentation.dataspace.copernicus.eu/APIs/OData.html#product-download)

## GCS

The downloader service must have the rights to read files on buckets.

`gs-provider-buckets` workflow and downloader arguments must be defined.

List of constellation:bucket comma-separated. 

bucket can contain several {IDENTIFIER} than will be replaced according to the sceneName. 

IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)

For more information see: [GCS Documentation](https://cloud.google.com/storage)

## Local directory

No credentials needed.

`local-path` downloader argument must be defined (local path where images are stored)

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


## ASF

Asf account credentials are needed.

`asf-token` downloader argument must be defined.

Asf uses `https://datapool.asf.alaska.edu/SLC/S{MISSION_VERSION}/{SCENE}.zip` for SLC product and `https://datapool.asf.alaska.edu/GRD-HD/S{MISSION_VERSION}/{SCENE}.zip` for GRD products.

[ASF Documentation](https://asf.alaska.edu/data-sets/sar-data-sets/sentinel-1/sentinel-1-documents-tools/)
 

## Landsat AWS

AWS credentials are needed (pay-on-request):
- `--landsat-aws-access-key-id`: Landsat AWS access key id
- `--landsat-aws-secret-access-key`: Landsat AWS secret access key

[Landsat AWS](https://registry.opendata.aws/usgs-landsat/)

## USGS M2M

USGS M2M account credentials are needed.

`usgs-m2m-username` and `usgs-m2m-token` downloader arguments must be defined.

An optional `usgs-m2m-cache-uri` can be specified to cache the API authentication token (e.g., `/tmp/m2m_cache` or `gs://bucket/cache`). Token is valid for 2 hours.

USGS M2M uses the [USGS Machine-to-Machine (M2M) API](https://m2m.cr.usgs.gov/api/docs/json/) to fetch Landsat Collection 2 Level-1 data.

> Note: An access request is necessary at https://ers.cr.usgs.gov/profile/access

For more information see: [USGS M2M API Documentation](https://m2m.cr.usgs.gov/api/docs/reference/#download-options)

