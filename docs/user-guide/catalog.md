# Catalogue

Catalogue component makes an inventory of all the scenes (and bursts for S1 images) covering the AOI between startDate and endDate. 

Some providers are implemented in order to list scenes and retrieve scenes metadata (Name, ID, etc.):

- [Scihub](#scihub): sentinel1 & 2 scenes
- [Creodias](#creodias): sentinel1 & 2 scenes
- [OneAtlas](#oneatlas): PHR & SPOT scenes
- [Creodias](#creodias), [GCS or AWS](#object-storage) and : for retrieving the Sentinel-1 annotations

## Sentinel constellations
### Scihub

Scihub can be used to list the Sentinel products. It require authentication using `scihub-username` and `scihub-password` catalogue arguments.

First, [ApiHub](https://apihub.copernicus.eu/apihub/search) is used to request Scihub catalogue. In case of failure, [DHUS](https://scihub.copernicus.eu/dhus/search) is used instead.

For more information see: [Scihub OpenSearch API Documentation](https://scihub.copernicus.eu/userguide/OpenSearchAPI)

### Creodias

No authentication required.

NB: Creodias is usually more reliable than Scihub, but Sentinel-1 catalogue returns less information than the Scihub's one.

For more information see: [Creodias API](https://creodias.eu/data-offer)

## Airbus constellations

### OneAtlas

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

## Sentinel-1 bursts annotations

To list the bursts of a Sentinel-1 product without downloading the file, the catalogue has to download the annotation file included in the .SAFE file. 

### Creodias

Creodias offers a service to get annotation files without downloading the whole product. Nevertheless, it's not always available on offline products.

### Object storage

GCS or AWS can be used to retrieve burst annotations from archives (.SAFE.zip) stored in a user bucket.

User account must have the appropriate rights to access the bucket (`-gcs-annotations-bucket`) (AWS to be added).

## Workflow steps

1. Catalogue will generate list of scenes matching with `geometry`, `start_time`, `end_time` and `scene_type` filter.
2. Catalogue will generate list of tiles for every scene (For Sentinel1: Burst = Tiles, for other constellation Scenes = Tiles)
3. Catalogue will check Geocube parameters validity (ie. `layers` JSON block which is reference Geocube variables and instances to use: must be existed)
4. Catalogue will create associated records. If record already exists, reuse the records (`record_tags` is use)
5. Workflow is started, Downloader will start one job per Scenes. After that, Processor will start also one job per Scenes/Tiles.

## Outputs

Returns list of Scenes with associated Tiles to download and eventually pre-process.



