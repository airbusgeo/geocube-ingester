# Prerequisites

The ingester needs:

- a relational database (currently supported Postgresql >= 11)
- a messaging System to exchange messages between services (currently supported: Pub/Sub, PGQueue: based on postgresql)
- an Object Storage, readable and writable (currently supported: local storage or GCS)
- a catalogue service, to get the list of products available over an AOI (currently supported: Scihub for Sentinel 1 and 2 with an appropriate user account)
- an image provider, to download the products returned by the catalogue service (currently supported : PEPS, Creodias, Onda, Mundis, GCS, local provider for Sentinel 1 and 2 with an appropriate user account)


