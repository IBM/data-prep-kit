# Set up a Kubernetes clusters for KFP execution

## 📝 Table of Contents
- [A Kind deployment supported platforms](#kind_platforms)
- [Preinstalled software components](#preinstalled)
  - [A Kind deployment](#kind)
  - [An existing cluster](#existing_cluster)
- [Installation steps](#installation)
  - [Installation on an existing Kubernetes cluster](#installation_existing)
- [Clean up the cluster](#cleanup)

The project provides instructions and deployment automation to run all components in an all-inclusive fashion on a 
single machine using a [Kind cluster](https://kind.sigs.k8s.io/) and a local data storage ([MinIO](https://min.io/)).
However, this topology is not suitable for processing medium and large datasets, and deployment should be carried out 
on a real Kubernetes or OpenShift cluster. Therefore, we recommend using Kind cluster for only for local testing and 
debugging, not production loads. For production loads use a real Kubernetes cluster.

Running a Kind Kubernetes cluster with Kubeflow pipelines (KFP) and MinIO requires significant
memory. We recommend deploying it on machines with at least 32 GB of RAM and 8-9 CPU cores. RHEL OS requires 
more resources, e.g. 64 GB RAM and 32 CPU cores.

## A Kind deployment supported Platforms <a name = "kind_platforms"></a> 
Executing KFP, MinIO, and Ray on a single Kind cluster pushes the system to its load limits. Therefore, although we are 
working on extending support for additional platforms, not all platforms/configurations are currently supported.

| Operating System  | Container Agent | Support  | Comments | 
|:-----------------:|:---------------:|:--------:| :---------: |
| RHEL 7            |     any         |    -     | Kind [doesn't support](https://github.com/kubernetes-sigs/kind/issues/3311) RHEL 7 |
|      RHEL 8       |                 |          |
|     RHEL 9.4      |     Docker      |   Yes    |
|     RHEL 9.4      |     Podman      |    No    | Issues with Ray job executions
|   Ubuntu 24-04    |     Docker      |   Yes    | 
|   Ubuntu 24-04    |     Podman      |          |
|   Windows WSL2    |     Docker      |   Yes    |
|   Windows WSL2    |     Podman      |          |
|    MacOS amd64    |     Docker      |   Yes    |
|    MacOS amd64    |     Podman      |          |
|    MacOS arm64    |     Docker      |          |
|    MacOS arm64    |     Podman      |    No    | Issues with Ray job executions

## Preinstalled software components <a name = "preinstalled"></a> 

Depending on whether a Kind cluster or an existing Kubernetes cluster is used, different software packages need to be preinstalled.

### Kind deployment <a name = "kind"></a> 
The following programs should be manually installed:

- [Helm](https://helm.sh/docs/intro/install/) 3.10.0 or greater must be installed and configured on your machine.
- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) tool for running local Kubernetes clusters 0.14.0 or newer must be installed on your machine.
- [Kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) 1.26 or newer must be installed on your machine.
- [MinIO Client (mc)](https://min.io/docs/minio/kubernetes/upstream/index.html) must be installed on your machine. Please 
choose your OS system, and process according to "(Optional) Install the MinIO Client". You have to install the `mc` client only.
- [git client](https://git-scm.com/downloads), we use git client to clone installation repository
- [lsof](https://www.ionos.com/digitalguide/server/configuration/linux-lsof/) usually it is part of Linux or MacOS distribution.
- Container agent such as [Docker](https://www.docker.com/) or [Podman](https://podman-desktop.io/)

### Existing Kubernetes cluster <a name = "existing_cluster"></a> 
Deployment on an existing cluster requires less pre-installed software
Only the following programs should be manually installed:

- [Helm](https://helm.sh/docs/intro/install/) 3.10.0 or greater must be installed and configured on your machine.
- [Kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) 1.26 or newer must be installed on your machine, and be 
able to connect to the external cluster.
- Deployment of the test data requires [MinIO Client (mc)](https://min.io/docs/minio/kubernetes/upstream/index.html) Please 
choose your OS system, and process according to "(Optional) Install the MinIO Client". Only the `mc` client should be installed.

## Installation steps <a name = "installation"></a>

Before installation, you have to decide which KFP version do you want to use. 
In order to use KFP v2, please set the following environment variable:

```shell
export KFPv2=1
```

Now, you can create a Kind cluster with all required software installed using the following command: 

```shell
 make -C scripts/k8s-setup setup
```
from this main package directory.
If you do not want to upload the testing data into the locally deployed Minio, and reduce memory footprint, please set:
```bash
export POPULATE_TEST_DATA=0
```
You can access the KFP dashboard at http://localhost:8080/ and the MinIO dashboard at http://localhost:8090/

### Installation on an existing Kubernetes cluster <a name = "installation_existing"></a>
Alternatively you can deploy pipeline to the existing Kubernetes cluster. 

In order to execute data transformers on the remote Kubernetes cluster, the following packages should be installed on the cluster:

- [KubeFlow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/) (KFP). Currently, we use 
upstream Argo-based KFP v1.
- [KubeRay](https://docs.ray.io/en/latest/cluster/kubernetes/index.html) controller and 
[KubeRay API Server](https://ray-project.github.io/kuberay/components/apiserver/) 

You can install the software from their repositories, or you can use our installation scripts.

Once your local kubectl is configured to connect to the external cluster do the following:
```bash
export EXTERNAL_CLUSTER=1
make -C scripts/k8s-setup setup
```

- In addition, you should configure external access to the KFP UI (`svc/ml-pipeline-ui` in the `kubeflow` ns) and the Ray 
Server API (`svc/kuberay-apiserver-service` in the `kuberay` ns). Depends on your cluster and its deployment it can be 
LoadBalancer services, Ingresses or Routes. 

- Optionally, you can upload the test data into the [MinIO](https://min.io/) Object Store, deployed as part of KFP. In 
order to do this, please provide external access to the Minio (`svc/minio-service` in the `kubeflow` ns) and execute the 
following commands from the root directory: 
```shell
export MINIO_SERVER=<Minio external URL>
kubectl apply -f scripts/k8s-setup/s3_secret.yaml
scripts/k8s-setup/populate_minio.sh
```

## Clean up the cluster <a name = "cleanup"></a>
If you use an external Kubernetes cluster set the `EXTERNAL_CLUSTER` environment variable.

```shell
export EXTERNAL_CLUSTER=1
```
Now, you can cleanup the external or Kind Kubernetes clusters by running the following command:

```shell
make -C scripts/k8s-setup clean
```
