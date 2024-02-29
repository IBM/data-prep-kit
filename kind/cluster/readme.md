# Building kind cluster with everything installed

This is an instruction for manual build. Will be converted into make file later

## create cluster

Run the following command:

```shell
kind create cluster -n goofy --config /Users/boris/Projects/fm-data-engineering/kind/cluster/kind-cluster-config.yaml
```

## Install NGNIX

Install with the following command:

```shell
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
```

validate using

```shell
kubectl get pods -n ingress-nginx
```

## Install KFP

run the following:

```shell
export PIPELINE_VERSION=2.0.5
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=$PIPELINE_VERSION"
```

validate using
```shell
kubectl get pods -n kubeflow
```

and see the pods state. Wait for all pods to be running

the pod `proxy-agent-xxxx` is failing. See [here](https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#disable-the-public-endpoint) (this might be a bit involved) on how to disable it

Install [ingress](kfp_ingress.yaml)

go to http://localhost:8080 and watch the UI

## Install KubeRay

Run the following:

```shell
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version 1.0.0 --set image.pullPolicy=IfNotPresent --create-namespace 
```

Now we can install API server using:

```shell
helm install -f /Users/boris/Projects/fm-data-engineering/kind/cluster/api_server_values.yaml kuberay-apiserver kuberay/kuberay-apiserver -n kuberay --set image.pullPolicy=IfNotPresent 
```

If you want to enble apiserver externally, you can use this [ingress](apiserver_ingress.yaml)
