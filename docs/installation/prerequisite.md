# Prerequisites

The ingester needs:

- a relational database (currently supported Postgresql >= 11)
- a messaging System to exchange messages between services (currently supported: Pub/Sub, PGQueue: based on postgresql)
- an Object Storage, readable and writable (currently supported: local storage or GCS)
- a catalogue service, to get the list of products available over an AOI ([currently supported](../user-guide/catalog.md))
- an image provider, to download the products returned by the catalogue service ([currently supported](../user-guide/providers.md))


