# Building kind cluster with everything installed

This is an instruction for manual build. Will be converted into make file later

## create cluster

Run the following command:

```shell
kind create cluster -n goofy --config /Users/boris/Projects/fm-data-engineering/kind/cluster/kind-cluster-config.yaml
```

## Add NGNIX

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

invoke 
```shell
get pods -n kubeflow
```

and see the pods state. Wait for all pods to be running

the pod `proxy-agent-xxxx` is failing. See [here](https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#disable-the-public-endpoint)
how to disable it

Install [ingress](ingress.yaml)

go to http://localhost:8080 and watch the UI

