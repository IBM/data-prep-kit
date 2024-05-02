#!/usr/bin/env bash

op="$1"

source ../common.sh

SLEEP_TIME="${SLEEP_TIME:-30}"
MAX_RETRIES="${MAX_RETRIES:-5}"
EXIT_CODE=0

deploy() {
    sed -i.back "s/tag: v[0-9].*/tag: v${KUBERAY_APISERVER}/" "${ROOT_DIR}/hack/ray_api_server_values.yaml" || { echo "Error: Failed to update YAML file."; exit 1; }
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/ || { echo "Error: Failed to add Helm repo."; exit 1; }
    helm repo update || { echo "Error: Failed to update Helm repo."; exit 1; }
    helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version "${KUBERAY_OPERATOR}" --set image.pullPolicy=IfNotPresent --create-namespace || { echo "Error: Failed to install kuberay-operator."; exit 1; }
    helm install -f "${ROOT_DIR}/hack/ray_api_server_values.yaml" kuberay-apiserver kuberay/kuberay-apiserver -n kuberay --version "${KUBERAY_APISERVER}" --set image.pullPolicy=IfNotPresent || { echo "Error: Failed to install kuberay-apiserver."; exit 1; }
    echo "Finished KubeRay deployment."
}

wait_for_kuberay() {
    echo "Waiting for KubeRay deployment..."
    wait_for_pods "kuberay" "$MAX_RETRIES" "$SLEEP_TIME" || { echo "Error: KubeRay deployment unsuccessful. Not all pods running."; exit 1; }
}

delete() {
    helm uninstall kuberay-operator -n kuberay || { echo "Warning: Failed to uninstall kuberay-operator."; }
    helm uninstall kuberay-apiserver -n kuberay || { echo "Warning: Failed to uninstall kuberay-apiserver."; }
}

usage() {
    cat <<EOF
Usage: ./install_kuberay.sh [cleanup|deploy-wait|deploy]
EOF
}

case "$op" in
    cleanup)
        header_text "Uninstalling KubeRay"
        delete
        ;;
    deploy-wait)
        header_text "Deploying and waiting for KubeRay"
        deploy
        wait_for_kuberay
        ;;
    deploy)
        header_text "Deploying KubeRay"
        deploy
        ;;
    *)
        usage
        ;;
esac