#!/usr/bin/env bash

# This script manages the deployment of KubeRay components.

# Define global variables
op="$1"
SLEEP_TIME="${SLEEP_TIME:-30}"
MAX_RETRIES="${MAX_RETRIES:-5}"

# Define functions

# Function to deploy KubeRay
deploy() {
    # Update YAML file with the desired version
    sed -i.back "s/tag: v[0-9].*/tag: v${KUBERAY_APISERVER}/" "${ROOT_DIR}/hack/ray_api_server_values.yaml" || { echo "Error: Failed to update YAML file."; exit 1; }
    
    # Add Helm repo and update
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/ || { echo "Error: Failed to add Helm repo."; exit 1; }
    helm repo update || { echo "Error: Failed to update Helm repo."; exit 1; }
    
    # Install kuberay-operator
    helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version "${KUBERAY_OPERATOR}" --set image.pullPolicy=IfNotPresent --create-namespace || { echo "Error: Failed to install kuberay-operator."; exit 1; }
    
    # Install kuberay-apiserver
    helm install -f "${ROOT_DIR}/hack/ray_api_server_values.yaml" kuberay-apiserver kuberay/kuberay-apiserver -n kuberay --version "${KUBERAY_APISERVER}" --set image.pullPolicy=IfNotPresent || { echo "Error: Failed to install kuberay-apiserver."; exit 1; }
    
    echo "Finished KubeRay deployment."
}

# Function to wait for KubeRay deployment
wait_for_kuberay() {
    echo "Waiting for KubeRay deployment..."
    wait_for_pods "kuberay" "$MAX_RETRIES" "$SLEEP_TIME" || { echo "Error: KubeRay deployment unsuccessful. Not all pods running."; exit 1; }
}

# Function to uninstall KubeRay
delete() {
    helm uninstall kuberay-operator -n kuberay || { echo "Warning: Failed to uninstall kuberay-operator."; }
    helm uninstall kuberay-apiserver -n kuberay || { echo "Warning: Failed to uninstall kuberay-apiserver."; }
}

# Function to display script usage
usage() {
    cat <<EOF
Usage: ./install_kuberay.sh [cleanup|deploy-wait|deploy]
EOF
}

# Main script logic
case "$op" in
    cleanup)
        echo "Uninstalling KubeRay..."
        delete
        ;;
    deploy-wait)
        echo "Deploying and waiting for KubeRay..."
        deploy
        wait_for_kuberay
        ;;
    deploy)
        echo "Deploying KubeRay..."
        deploy
        ;;
    *)
        usage
        ;;
esac