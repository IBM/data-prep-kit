# Building Kind cluster with everything installed

This is a manual build instruction. As an alternative, you can execute the `make setup-cluster` makefile rule instead.

# Before you begin

Ensure that you have the following:

- [Helm](https://helm.sh/) 3.10.0 or greater must be installed and configured on your machine.
- [Kind](https://kind.sigs.k8s.io/) tool for running local Kubernetes clusters 0.14.0 or newer.
- [Kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) 1.26 or newer must be installed on your machine.


## Create cluster

Run the following command to create the cluster:

```shell
cd /tmp
git clone git@github.ibm.com:ai-models-data/fm-data-engineering.git
cd fm-data-engineering
ROOT_DIR=$PWD/kind/
kind create cluster --name goofy --config ${ROOT_DIR}/cluster/kind-cluster-config.yaml
```

## Install KFP

Install [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#deploying-kubeflow-pipelines) and wait for it to be ready:

```shell
    PIPELINE_VERSION=1.8.5
    cd /tmp
	git clone https://github.com/kubeflow/pipelines.git
	cd pipelines
	git checkout tags/${PIPELINE_VERSION}
	kubectl apply -k manifests/kustomize/cluster-scoped-resources
	kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
	sed -i '/inverse-proxy$/d' manifests/kustomize/env/dev/kustomization.yaml
    kubectl apply -k manifests/kustomize/env/dev
    kubectl wait --for=condition=ready --all pod -n kubeflow --timeout=240s
```
Add permissions with RBAC role:
```shell
    kubectl create clusterrolebinding pipeline-runner-extend --clusterrole cluster-admin --serviceaccount=kubeflow:pipeline-runner
```

Note that the pod `proxy-agent-xxxx` is failing. For instructions on disabling it, please refer to [this](https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#disable-the-public-endpoint) detailed guide. 


## Install KubeRay

Install Kuberay:

```shell
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version 1.0.0 --set image.pullPolicy=IfNotPresent --create-namespace 
```

Next install API server and wait for it to be ready:

```shell
helm install -f ${ROOT_DIR}/cluster/api_server_values.yaml kuberay-apiserver kuberay/kuberay-apiserver -n kuberay
kubectl wait --for=condition=ready --all pod -n kuberay --timeout=120s
```

## Install NGNIX

To access the API server and Kubeflow pipeline UI externally, we make use NGINX ingress.

Install [Ingress NGNIX](https://kind.sigs.k8s.io/docs/user/ingress/#ingress-nginx) and wait for it to be ready:

```shell
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

kubectl wait --namespace ingress-nginx \
          --for=condition=ready pod \
          --selector=app.kubernetes.io/component=controller \
          --timeout=90s
kubectl apply -f $ROOT_DIR/cluster/apiserver_ingress.yaml
kubectl apply -f $ROOT_DIR/cluster/kfp_ingress.yaml
```

Open the Kubeflow Pipelines UI at  http://localhost:8081/kfp/#/pipelines

Alternatively, port forwarding to localhost can be used to access the services externally:

```shell
kubectl port-forward service/ml-pipeline-ui -n kubeflow 8080:80 &

kubectl port-forward service/kuberay-apiserver-service -n kuberay 8081:8888 &
```

Open the Kubeflow Pipelines UI at http://localhost:8080/

## Cleanup

Stop kubectl port-forward processes (e.g., using pkill kubectl)

Delete the temporary directories created for installation during the setup:

```shell
rm -rf /tmp/fm-data-engineering
rm -rf /tmp/pipelines
```

delete the cluster:

```shell
kind delete cluster
```