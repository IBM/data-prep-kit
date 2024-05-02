#!/usr/bin/env bash

# Set default values if not provided
SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-20}"
EXIT_CODE=0

# Include common functions
source ../common.sh

# Functions

deploy_kubeflow() {
    local temp_dir
    temp_dir=$(mktemp -d)
    echo "Temporary dir: $temp_dir"
    cd "$temp_dir" || exit 1
    git clone https://github.com/kubeflow/pipelines.git
    cd pipelines || exit 1
    git checkout "tags/${PIPELINE_VERSION}"
    kubectl apply -k manifests/kustomize/cluster-scoped-resources
    kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
    # Disable the public endpoint
    sed -i.back '/inverse-proxy$/d' manifests/kustomize/env/dev/kustomization.yaml
    sed -i.back 's/30Mi/60Mi/' manifests/kustomize/third-party/application/application-controller-deployment.yaml
    sed -i.back 's/20Mi/60Mi/' manifests/kustomize/third-party/application/application-controller-deployment.yaml
    deploy_with_retries "-k" "manifests/kustomize/env/dev" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?
    if [[ $EXIT_CODE -ne 0 ]]; then
        echo "Kubeflow deployment unsuccessful."
        exit 1
    fi
    kubectl create clusterrolebinding pipeline-runner-extend --clusterrole cluster-admin --serviceaccount=kubeflow:pipeline-runner
    echo "Finished Kubeflow deployment."
    rm -rf "$temp_dir"
}

wait_for_kubeflow() {
    echo "Waiting for Kubeflow deployment..."
    wait_for_pods "kubeflow" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?
    if [[ $EXIT_CODE -ne 0 ]]; then
        echo "Kubeflow deployment unsuccessful. Not all pods running."
        exit $EXIT_CODE
    fi
    # Disable cache for testing
    kubectl patch mutatingwebhookconfiguration cache-webhook-kubeflow --type='json' -p='[{"op":"replace", "path": "/webhooks/0/rules/0/operations/0", "value": "DELETE"}]'
}

delete_kubeflow() {
    kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION"
    kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=$PIPELINE_VERSION"
}

print_usage() {
    cat <<EOF
Usage: $0 [cleanup|deploy-wait|deploy]
EOF
}

# Main

case "$1" in
    cleanup)
        header_text "Uninstalling Kubeflow"
        delete_kubeflow
        ;;
    deploy-wait)
        header_text "Waiting for Kubeflow deployment"
        wait_for_kubeflow
        ;;
    deploy)
        header_text "Installing Kubeflow"
        deploy_kubeflow
        ;;
    *)
        print_usage
        exit 1
        ;;
esac