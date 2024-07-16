# Building Kind cluster with everything installed

## Pre-requirements

- [Supported platforms](../../kfp/doc/setup.md#kind_platforms)
- [Preinstalled software components](../../kfp/doc/setup.md#kind)
- [Preparing a Kind cluster for testing](../../kfp/doc/setup.md#installation)

As an alternative, you can execute the following manual installation instructions:

### Create cluster

Run the following command to create the cluster:

```shell
cd /tmp
git clone https://github.com/IBM/data-prep-kit.git
cd data-prep-kit
export REPOROOT=$PWD
kind create cluster --name dataprep --config ${REPOROOT}/scripts/k8s-setup/kind-cluster-config.yaml
```

Note that by default this will create a kind cluster with 2 worker nodes. If you would like a different
amount of node, modify [cluster configuration](./kind-cluster-config.yaml)

### Install KFP

Install [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#deploying-kubeflow-pipelines) and wait for it to be ready:

```shell
# Set required KFP version. You can reference to the latest supported version in the [requirements.env](./requirements.env) file.
# Currently, we support 1.8.5 for KFPv1 and 2.2.0 for KFP v2
export PIPELINE_VERSION=1.8.5
cd $REPOROOT/scripts/k8s-setup/tools/ && ./install_kubeflow.sh deploy && cd -
kubectl wait --for=condition=ready --all pod -n kubeflow --timeout=300s
```

### Install KubeRay

Install Kuberay:

```shell
cd $REPOROOT/scripts/k8s-setup/tools/ && KUBERAY_APISERVER=1.1.0 KUBERAY_OPERATOR=1.0.0 ./install_kuberay.sh deploy && cd -
kubectl wait --for=condition=ready --all pod -n kuberay --timeout=300s
```


### Install NGNIX

To access the API server and Kubeflow pipeline UI externally, we make use NGINX ingress.

Install [Ingress NGNIX](https://kind.sigs.k8s.io/docs/user/ingress/#ingress-nginx) for KFP, RAY and MinIO and wait for it to be ready:

```shell
${REPOROOT}/scripts/k8s-setup/tools/install_nginx.sh deploy
kubectl wait --namespace ingress-nginx \
          --for=condition=ready pod \
          --selector=app.kubernetes.io/component=controller \
          --timeout=90s
```

To deploy the ingress for Ray API Server, KFP and MinIO execute the following:
```shell
kubectl apply -f $REPOROOT/scripts/k8s-setup/ray_api_server_ingress.yaml
kubectl apply -f $REPOROOT/scripts/k8s-setup/kfp_ingress.yaml
kubectl apply -f $REPOROOT/scripts/k8s-setup/minio_ingress.yaml
```

Open the Kubeflow Pipelines UI at  http://localhost:8080/


### Working with a MinIO server instead of S3 storage
You can work with a real S3 storage, but for testing you can use the Mino server which is deployed as part of the KFP
installation. You can access the Minio dashboard at http://localhost:8090/

#### Create a secret
The MinIO service, deployed as a part of KFP, uses a username (`minio`) as an access_key/password (`minio123`)
as the secret key.
A secret needs to be created for accessing MinIO using the following command:

```shell
kubectl apply -f $REPOROOT/scripts/k8s-setup/s3_secret.yaml
```

#### Copy test data

Populating Minio server with test data can be done using `mc`. Use the following command:

```shell
$REPOROOT/scripts/k8s-setup/populate_minio.sh
```

This file creates an mc alias, creates the test bucket and copies the local test data into MinIO. If you need
to load additional data, please load it using additional `mc` commands, similar to the ones being
used by `populate_minio.sh`


## Cleanup
See [Clean up the cluster](../../kfp/doc/setup.md#cleanup)
