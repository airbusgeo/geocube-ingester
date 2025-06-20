# Catalogue

> NB: This documentation is for user that want to use the Catalogue. For documentation on how to implement a new catalogue, see [Developer-Guide/Catalogue](#developer-guide/catalog.md).

The Catalogue component makes an inventory of all the scenes (and bursts for S1 images) covering the AOI between startDate and endDate, depending on user-defined criteria. The results, including the product metadata, are formated in a standard way, following the GeoJSON standard and ready to be ingested.

See [payload](payload.md) to create a payload.

List of implemented catalogues :

- [Copernicus](#copernicus): sentinel1 & 2 scenes
- [Creodias](#creodias): sentinel1 & 2 scenes
- [Landsat AWS](#landsat-aws): Landsat 8 & 9
- [OneAtlas](#oneatlas): PHR & SPOT scenes
- [GCS or AWS](#object-storage) : to retrieve the Sentinel-1 annotations


## Constellations
### Sentinel constellations
#### Copernicus

Supported constellations:

- `sentinel1`
- `sentinel2`

Copernicus can be used to list the Sentinel products. It does not require authentication.

Use the `--copernicus-catalog` flag to enable this catalogue.

For more information see: [Copernicus OpenSearch API Documentation](https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html)  [Copernicus ODATA API Documentation](https://documentation.dataspace.copernicus.eu/APIs/OData.html)

#### Creodias

Supported constellations:

- `sentinel1`
- `sentinel2`

No authentication required.

Use the `--creodias-catalog` flag to enable this catalogue.

> NB: Creodias is usually more reliable than Copernicus, but Sentinel-1 catalogue returns less information than the Copernicus' one.

For more information see: [Creodias API](https://creodias.eu/data-offer)

### USGS constellations
#### Landsat AWS

Supported constellations:

- `landsat89`

It uses the STAC interface to list the Landsat products available on AWS.

No authentication required.

Use the `--landsat-aws-catalog` flag to enable this catalogue.
For more information see: [USGS Landsat](https://registry.opendata.aws/usgs-landsat/)

### Airbus constellations

#### OneAtlas

Supported constellations:

- `spot`
- `pleiades`/`phr`


Use the following arguments to configure this catalogue:
- `oneatlas-username`
- `oneatlas-apikey`
- `oneatlas-endpoint`
- `oneatlas-order-endpoint`
- `oneatlas-auth-endpoint`

#### Account

In order to use oneAtlas, you need to create an account [here](https://account4.intelligence-airbusds.com/account/CreateAccount.aspx?l=fr&RelayState=). But if you would like to give our service a try before purchasing, 
you can sign up for a 1 month Free Trial by signing up on our website [here](https://oneatlas.airbus.com/home).

Concerning authentication, you need to create an APIKEY [here](https://connect4.intelligence-airbusds.com/adfs/ls/) (more documentation is available [here](https://api.oneatlas.airbus.com/guides/g-authentication/))
Shortly after you can learn about managing your account and subscriptions through our [Manage Contract Guide](https://api.oneatlas.airbus.com/guides/oneatlas-data/g-manage-contract/).

Once your account is created you should be ready to search! Take a look at our image catalog, the ‘Living Library’. 
High resolution images are added continuously on a daily basis. It is designed to offer an extensive set of search criteria which you can find in our [Search Guide](https://api.oneatlas.airbus.com/guides/oneatlas-data/g-search/). 

#### Imagery

OneAtlas Catalog is requested in order to download PHR, SPOT Products in Dimap format. 
Catalog provides an estimated cost of a potential processing order (available in ScenesInventory)

### Sentinel-1 bursts annotations

To list the bursts of a Sentinel-1 product without downloading the file, the catalogue has to download the annotation file included in the .SAFE file. 

#### Object storage

Local storage, GCS or AWS can be used to retrieve burst annotations from archives (.SAFE.zip) stored in a user bucket.

User account must have the appropriate rights to access the bucket (`-annotations-urls`).

## Outputs

It returns a list of Scenes with associated Tiles, ready to be ingested.

For each scene, metadata provided by the catalogue are added to the tags of the `record` definining the scene:

	- `sourceID`: Name of the product
	- `uuid`: Universally unique identifier in the catalogue (if exists)
	- `productType`: Type of the product, containing the sensor and the level of processing (e.g. `S2MSIL1C` for Sentinel2, `LC_C2_L1TP` for Landsat)
	- `ingestionDate`: Date-time of acquisition
	- `constellation`
	- `satellite`
	- `orbit`: Absolute orbits is the number of orbits taken from the beginning of the mission.
	- `orbitDirection`
	- `relativeOrbit`:  Relative orbit tells you where you are in the repeating orbit-cycle.
	- `downloadURL` (if provided)
	- `sunAzimuth`, `sunElevation`
	- `incidenceAngle`, `incidenceAzimuth`

For optical products:

	- `cloudCoverPercentage`
	- `landCloudCoverPercentage`: only clouds covering land

For SAR products:

	- `polarisationMode`
	- `sliceNumber` (Sentinel1)
	- `lastRelativeOrbit` (Sentinel1)
	- `lastOrbit` (Sentinel1)

