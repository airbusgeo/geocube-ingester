# Geocube-Ingester

Geocube-Ingester is an example of an automatic and parallel ingester for the [Geocube](https://github.com/airbusgeo/geocube).
It is provided either as a generic framework for Geocube ingestion or as an example to develop new ingesters.

From an AOI, a time interval and a set of parameters (to configure the output layers), the ingester takes care of everything from the downloading of the products, the computing, its parallelization and the ingestion in the Geocube.

![Workflow](architecture/IngesterWorkflow.png)

It currently supports Sentinel-1, Sentinel-2, Pléiades and SPOT and it's designed to easily add new sources of data or satellites using the [interfaces](architecture/interfaces.md).

Dockerfiles are provided to do automatic preprocessing of images using **user-defined SNAP-Processing graphs**, **python script** or **docker commands**.


