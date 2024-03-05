#!/usr/bin/env bash

op=$1

source ${ROOT_DIR}/hack/common.sh

kind_delete() {
  kind delete cluster --name goofy
}

kind_create() {
  kind create cluster --name goofy --config ${ROOT_DIR}/hack/kind-cluster-config.yaml
}

case "$op" in
cleanup)
  header_text "Uninstalling kind cluster"
  kind_delete kind || true
  ;;
*)
  header_text "Installing kind cluster"
  kind_create kind
  ;;
esac
