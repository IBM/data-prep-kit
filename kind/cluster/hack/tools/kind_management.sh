#!/usr/bin/env bash

op=$1; shift
cluster_name="${1:-goofy}"

source ${ROOT_DIR}/hack/common.sh

kind_delete() {
  kind delete cluster --name $cluster_name
}

kind_create() {
  kind create cluster --name $cluster_name --config ${ROOT_DIR}/hack/kind-cluster-config.yaml
}

usage(){
        cat <<EOF
"Usage: ./kind_management.sh [create_cluster|delete_cluster]"
EOF
}

case "$op" in
delete_cluster)
  header_text "Uninstalling kind cluster"
  kind_delete kind || true
  ;;
create_cluster)
  header_text "Installing kind cluster"
  kind_create kind
  ;;
 *)
  usage
  ;;
esac
