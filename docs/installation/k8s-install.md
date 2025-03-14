# Installation - Kubernetes Cluster

## Prerequisites

Kubctl must be installed and configured in order to be connected to the right kubernetes cluster.
You also need database and Messaging (kubernetes examples are available in Geocube Installation Guide) these same examples can be used here.
Ingester should be deployed in the same cluster as Geocube, in a different namespace.

### IAM & Security

All the notions of security and service account are not covered in this document. It is the responsibility of the installers.
The files presented below are available as examples/templates. They do not present any notions of security.

### Container Registry

You can create your own registry server: https://docs.docker.com/registry/deploying/

#### Private Registry

You can configure your kubernetes deployment files with private docker registry.

For more information, see: https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/

`imagePullSecrets` is defined in your kubernetes configuration files and image name is specified as follow ex: `image: geocube-private-image:tag`

#### Docker Hub

In case the images are stored on https://hub.docker.com, you can define them as follows in your kubernetes configuration files (postgresql example: `image: postgres:11`):

```kubernetes
apiVersion: networking.k8s.io/v1
kind: Deployment
metadata:
  name: postgresql
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: postgresql
          image: postgres:11
```

In this example, https://hub.docker.com/layers/postgres/library/postgres/11.0/images/sha256-05f9b83f85bdf0382b1cb8fb72d17d7c8098b0287d7dd1df4ff09aa417a0500b?context=explore image will be loaded.

## Create Namespace

You must define a CLUSTER variable environment.
The dedicated namespace `ingester` is created by running the following command:

```bash
$ kubectl apply -f deploy/k8s/namespace.yaml
```

## Create Secrets in namespace

Kubernetes configuration file is available here: `deploy/k8s/secrets.yaml`. All the parameters between `{{}}` are mandatory:
1. `{{GEOCUBE_SERVER}}`: uri of the Geocube (eg. `127.0.0.1:8080`)
2. `{{STORAGE_URI}}`: uri where to store the outputs (layers) of the Ingester (eg. `/ingester/` or `gs://ingester/`)
3. `{{DB_CONNECTION}}`: uri of the database (eg. `postgresql://user:password@localhost:5432/geocube`)

The other parameters (mainly authentication information for image providers) are optional.

Ingester server must have sufficient rights in order to read and write into database. For more information, see: https://www.postgresql.org/docs/11/auth-pg-hba-conf.html

After that, secrets can be created:

```bash
$ kubectl apply -f deploy/k8s/public/secrets.yaml
```

## Create deployment in namespace

In order to start Ingester, you have to define some parameters in `deploy/k8s/public/workflow.yaml` (all the parameters between `{{}}` are mandatory):

1. `{{WORKFLOW_IMAGE}}`: Workflow docker image  (eg. `<container_registry>/processor:<tag>`)
3. `{{DOWNLOADER_IMAGE}}`: Downloader docker image  (eg. `<container_registry>/downloader:<tag>`)
4. `{{PROCESSOR_IMAGE}}`: Processor docker image  (eg. `<container_registry>/processor:<tag>`)

Then, deployement can be created in namespace `ingester` by running the following command:

```bash
$ kubectl apply -f deploy/k8s/public/workflow.yaml
```

NB:

- Workflow.yaml kubernetes file is an example of Ingester deployment in the cloud. You need to adapt them in order to configure database, messaging and storage access.
- In you want to use pubsub emulator, you need to add `PUB_SUB_EMULATOR` variable environment in your deployment and replication controller (already describe in Geocube Documentation).
- If you want to use pgqueue, you need to refer to: [PGQueue Configuration](local-install.md#PGQueue)

Ex configuration with pubSub emulator:

```kubernetes
env:
  - name: PUBSUB_EMULATOR_HOST
    value: 0.0.0.0:8085
```
