#!/usr/bin/env bash

op=$1; shift
cluster_name="${1:-dataprep}"

source ../common.sh

kind_delete() {
  kind delete cluster --name $cluster_name
}

kind_create() {
  if ! command -v lsof &> /dev/null ; then
      echo "lsof could not be found. Please install it and try again"
      exit 1
  fi
  if ! command -v kind &> /dev/null ; then
      echo "kind could not be found. Please install it and try again"
      exit 1
  fi
  if ! command -v helm &> /dev/null ; then
      echo "helm could not be found. Please install it and try again"
      exit 1
  fi
  if ! command -v kubectl &> /dev/null ; then
      echo "kubectl could not be found. Please install it and try again"
      exit 1
  fi
  if ! command -v mc &> /dev/null ; then
      echo "mc could not be found"
      exit 1
  fi
  if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null ; then
      echo "port 8080 is in use, please clear the port and try again"
      exit 1
  fi
  if lsof -Pi :8090 -sTCP:LISTEN -t >/dev/null ; then
      echo "port 8090 is in use, please clear the port and try again"
      exit 1
  fi
  kind create cluster --name $cluster_name --config ${K8S_SETUP_SCRIPTS}/kind-cluster-config.yaml
}

usage(){
        cat <<EOF
"Usage: ./kind_management.sh [create_cluster|delete_cluster]"
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