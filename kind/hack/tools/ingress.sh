#!/usr/bin/env bash

set -euo pipefail

readonly ROOT_DIR="/path/to/root/dir"

source "${ROOT_DIR}/hack/common.sh"

deploy_ingress() {
    kubectl apply -f "${ROOT_DIR}/hack/ray_api_server_ingress.yaml"
    if [[ "${DEPLOY_KUBEFLOW:-0}" -eq 1 ]]; then
        kubectl apply -f "${ROOT_DIR}/hack/kfp_ingress.yaml"
    fi
}

delete_ingress() {
    kubectl delete -f "${ROOT_DIR}/hack/ray_api_server_ingress.yaml"
    if [[ "${DEPLOY_KUBEFLOW:-0}" -eq 1 ]]; then
        kubectl delete -f "${ROOT_DIR}/hack/kfp_ingress.yaml"
    fi
}

usage() {
    cat <<EOF
Usage: $0 [deploy|cleanup]
EOF
}

main() {
    local operation="${1:-}"
    case "$operation" in
        cleanup)
            header_text "Uninstalling NGINX"
            delete_ingress
            ;;
        deploy)
            header_text "Installing NGINX"
            deploy_ingress
            ;;
        *)
            usage
            ;;
    esac
}

main "$@"