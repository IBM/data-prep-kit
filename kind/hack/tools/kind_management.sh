#!/usr/bin/env bash

set -euo pipefail

op="$1"; shift
cluster_name="${1:-dataprep}"

source ../common.sh

check_dependencies() {
  local dependencies=("lsof" "kind" "helm" "kubectl" "wget" "mc")
  for dep in "${dependencies[@]}"; do
    if ! command -v "$dep" &>/dev/null; then
      echo "$dep could not be found. Please install it and try again"
      exit 1
    fi
  done
}

check_port_availability() {
  if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null; then
    echo "Port 8080 is in use, please clear the port and try again"
    exit 1
  fi
}

kind_delete() {
  kind delete cluster --name "$cluster_name"
}

kind_create() {
  check_dependencies
  check_port_availability
  kind create cluster --name "$cluster_name" --config "${ROOT_DIR}/hack/kind-cluster-config.yaml"
}

usage(){
  cat <<EOF
Usage: $0 [create_cluster|delete_cluster]
EOF
}

case "$op" in
  delete_cluster)
    header_text "Uninstalling kind cluster"
    kind_delete || true
    ;;
  create_cluster)
    header_text "Installing kind cluster"
    kind_create
    ;;
  *)
    usage
    ;;
esac
