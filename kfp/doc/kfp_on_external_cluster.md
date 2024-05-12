# Deployment Data Transformer Pipelines on an External Kubernetes Cluster

The project provides instructions and deployment automation to run all components in an all-inclusive fashion on a 
single machine using a Kind cluster. However, this topology is not suitable for processing medium and large datasets, 
and deployment should be carried out on a real Kubernetes cluster.

This document provides instructions on how to do so.

## Pre-requirements
The following programs should be manually installed:

- [Helm](https://helm.sh/docs/intro/install/) 3.10.0 or greater must be installed and configured on your machine.
- [Kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) 1.26 or newer must be installed on your machine, and be 
able to connect to the external cluster. For OpenShift clusters OpenShift CLI 
[oc](https://docs.openshift.com/container-platform/4.15/cli_reference/openshift_cli/getting-started-cli.html) can be used instead.
- [wget](https://www.gnu.org/software/wget/) 1.21 must be installed on your machine.

## Installation steps
In order to execute data transformers on the remote cluster, the following packages should be installed on the Kubernetes cluster:
-KubeFlow Pipelines ([KFP](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/)). Currently, we use 
upstream Argo-based KFP v1.
- [KubeRay](https://docs.ray.io/en/latest/cluster/kubernetes/index.html) controller and 
[KubeRay API Server](https://ray-project.github.io/kuberay/components/apiserver/) 
You can install the software from their repositories, or you can use our installation scripts. If your local kubectl is 
configured to connect to the external cluster do the following:
```bash
export EXTERNAL_CLUSTER=1
make setup
```

- in addition, you should configure external access to the KFP UI (`svc/ml-pipeline-ui` in `kubeflow` ns) and the Ray 
Server API (`svc/kuberay-apiserver-service` in `kuberay` ns). Depends on your cluster and its deployment it can be 
LoadBalancer services, Ingresses or Routes. 

## Installation and Running Pipelines
When you achieved access to the KFP UI, work according to 
[Compiling pipeline and deploying it to KFP](./simple_transform_pipeline.md#compiling-pipeline-and-deploying-it-to-kfp)
