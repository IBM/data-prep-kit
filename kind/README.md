# Building Kind cluster with everything installed

## Pre-requirements

### Supported platforms
A Kind cluster is not intended for production purposes; it is only meant as a local execution example. However,
running a Kind Kubernetes cluster with KubeFlow pipelines (KFP) and local data storage (Minio) requires significant
memory. Therefore, we recommend deploying it on machines with at least 32 GB of RAM and 8-9 CPU cores. RHEL OS requires 
more resources, e.g. 64 GB RAM and 32 CPU cores.

> **Note**: for MacOS users, see the following [comments](../doc/mac.md)

### Preinstalled software
The following programs should be manually installed:

- [Helm](https://helm.sh/docs/intro/install/) 3.10.0 or greater must be installed and configured on your machine.
- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) tool for running local Kubernetes clusters 0.14.0 or newer must be installed on your machine.
- [Kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) 1.26 or newer must be installed on your machine.
- [MinIO Client (mc)](https://min.io/docs/minio/kubernetes/upstream/index.html) must be installed on your machine. Please 
choose your OS system, and process according to "(Optional) Install the MinIO Client". You have to install the `mc` client only.
- [git client](https://git-scm.com/downloads), we use git client to clone installation repository
- [lsof](https://www.ionos.com/digitalguide/server/configuration/linux-lsof/) usually it is part of Linux or MacOS distribution.
- Container agent such as [Docker](https://www.docker.com/) or [Podman](https://podman-desktop.io/)

> **NOTE**: Additionally, ensure that nothing is running on port 8080 and 8090, which is used by the Kind cluster ingress.

If you do not want to upload the testing data into the local Minio, and reduce memory footprint, please set:
```bash
export POPULATE_TEST_DATA ?= 0
```

## Preparing Kind cluster for testing

This is a manual build instruction. As an alternative, you can execute the `make setup` makefile rule in
the project `kind` directory instead. `make setup` performs complete installation, including validation that port
8080 and 8090 is available, creation of the cluster, installing required software (NGNIX, KubeRay and KFP), creating
ingresses and secrets and loading local data to Minio.

### Create cluster

Run the following command to create the cluster:

```shell
cd /tmp
git clone git@github.com:IBM/data-prep-kit.git
cd data-prep-kit
ROOT_DIR=$PWD/kind/
kind create cluster --name dataprep --config ${ROOT_DIR}/hack/kind-cluster-config.yaml
```

Note that by default this will create a kind cluster with 2 worker nodes. If you would like a different
amount of node, modify [cluster configuration](hack/kind-cluster-config.yaml)

### Install KFP

Install [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#deploying-kubeflow-pipelines) and wait for it to be ready:

```shell
PIPELINE_VERSION=1.8.5
cd /tmp
git clone https://github.com/kubeflow/pipelines.git
cd pipelines
git checkout tags/${PIPELINE_VERSION}
kubectl apply -k manifests/kustomize/cluster-scoped-resources
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
sed -i.back '/inverse-proxy$/d' manifests/kustomize/env/dev/kustomization.yaml
kubectl apply -k manifests/kustomize/env/dev
kubectl wait --for=condition=ready --all pod -n kubeflow --timeout=300s
```
Add permissions with RBAC role:
```shell
kubectl create clusterrolebinding pipeline-runner-extend --clusterrole cluster-admin --serviceaccount=kubeflow:pipeline-runner
```

### Install KubeRay

Install Kuberay:

```shell
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version 1.0.0 --set image.pullPolicy=IfNotPresent --create-namespace 
```

Next install API server and wait for it to be ready:

```shell
helm install -f ${ROOT_DIR}/hack/ray_api_server_values.yaml kuberay-apiserver kuberay/kuberay-apiserver --version 1.1.0 -n kuberay
kubectl wait --for=condition=ready --all pod -n kuberay --timeout=120s
```

### Install NGNIX

To access the API server and Kubeflow pipeline UI externally, we make use NGINX ingress.

Install [Ingress NGNIX](https://kind.sigs.k8s.io/docs/user/ingress/#ingress-nginx) for KFP and RAY and wait for it to be ready:

```shell
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

kubectl wait --namespace ingress-nginx \
          --for=condition=ready pod \
          --selector=app.kubernetes.io/component=controller \
          --timeout=90s
```

Install [Ingress NGNIX](https://kind.sigs.k8s.io/docs/user/ingress/#ingress-nginx) for Minio and wait for it to be ready:

```shell
kubectl apply -f $ROOT_DIR/hack/nginx_deploy_minio.yaml
kubectl wait --namespace ingress-nginx-minio \
          --for=condition=ready pod \
          --selector=app.kubernetes.io/component=controller \
          --timeout=90s
```

To deploy the ingress for ray apiserver, kfp and Minio execute the following:
```shell
kubectl apply -f $ROOT_DIR/hack/ray_api_server_ingress.yaml
kubectl apply -f $ROOT_DIR/hack/kfp_ingress.yaml
kubectl apply -f $ROOT_DIR/hack/minio_ingress.yaml
```

Open the Kubeflow Pipelines UI at  http://localhost:8080/

Finally a secret need to be created for accessing Minio using the following command:

```shell
kubectl apply -f $ROOT_DIR/hack/s3_secret.yaml
```

### Working with a Minio server instead of S3 storage
You can work with a real S3 storage, but for testing you can use the Mino server which is deployed as part of the KFP
installation.

#### Copy test data

Populating Minio server with test data can be done using `mc`. Use the following command:

```shell
./hack/populate_minio.sh
```

This file create an mc alias, create the test bucket and copy local data to Minio. If you need
to load additional data, please load it using additional `mc` commands, similar to the ones being
used by `populate_minio.sh`

Finally, note, that kfp version of Minio, is using a username (`minio`) as an access key and password ('minio123')
as the secret. The secret to use for Minio access is located in `kubeflow` ns with the name `s3-secret`

## Cleanup

delete the cluster:

```shell
kind delete cluster --name dataprep
```

alternatively you can execute

```shell
make clean
```
