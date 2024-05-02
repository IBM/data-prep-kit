#!/usr/bin/env bash

operation="$1"

SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-10}"
EXIT_CODE=0

source ../common.sh

deploy_nginx() {
    kubectl apply -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml"
}

wait_for_nginx_deployment() {
    echo "Waiting for NGINX deployment..."
    wait_for_pods "ingress-nginx" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?
    
    if [[ $EXIT_CODE -ne 0 ]]; then
        echo "NGINX deployment unsuccessful. Not all pods are running."
        exit "$EXIT_CODE"
    fi
}

uninstall_nginx() {
    kubectl delete -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml"
}

print_usage() {
    cat <<EOF
Usage: ./install_nginx.sh [cleanup|deploy-wait|deploy]
EOF
}

case "$operation" in
    cleanup)
        header_text "Uninstalling NGINX"
        uninstall_nginx
        ;;
    deploy-wait)
        header_text "Deploying and waiting for NGINX"
        deploy_nginx
        wait_for_nginx_deployment
        ;;
    deploy)
        header_text "Deploying NGINX"
        deploy_nginx
        ;;
    *)
        print_usage
        ;;
esac
