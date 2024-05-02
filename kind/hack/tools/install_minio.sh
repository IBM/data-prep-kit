#!/usr/bin/env bash

op=$1

source ../common.sh

SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-20}"
EXIT_CODE=0

deploy_minio() {
  kubectl apply -f "${ROOT_DIR}/hack/s3_secret.yaml"
  kubectl apply -f "${ROOT_DIR}/hack/minio_ingress.yaml"
}

wait_for_minio() {
  echo "Waiting for Minio server"
  ns="kubeflow"
  while [[ $(kubectl get ingress minio -n "$ns" -o jsonpath="{.status.loadBalancer.ingress[0].hostname}") != "localhost" ]]; do
    echo "Still waiting for Minio ingress to get ready"
    sleep 20
  done
  echo "Ingress Minio is ready"
}

delete_minio() {
  kubectl delete -f "${ROOT_DIR}/hack/s3_secret.yaml"
  kubectl delete -f "${ROOT_DIR}/hack/minio_ingress.yaml"
}

usage() {
  cat <<EOF
Usage: ./install_minio.sh [cleanup|deploy-wait|deploy]
EOF
}

case "$op" in
  cleanup)
    header_text "Uninstalling Minio"
    delete_minio || exit 1
    ;;
  deploy-wait)
    header_text "Wait for Minio deployment"
    wait_for_minio || exit 1
    ;;
  deploy)
    header_text "Installing Minio"
    deploy_minio || exit 1
    ;;
  *)
    usage
    exit 1
    ;;
esac
