#!/usr/bin/env bash

op=$1

source ${K8S_SETUP_SCRIPTS}/common.sh

deploy() {
  sleep 10
  kubectl apply -f ${K8S_SETUP_SCRIPTS}/ray_api_server_ingress.yaml
if [[ "${DEPLOY_KUBEFLOW}" -eq 1 ]]; then
	kubectl apply -f ${K8S_SETUP_SCRIPTS}/kfp_ingress.yaml
fi
}

delete(){
  kubectl delete -f ${K8S_SETUP_SCRIPTS}/ray_api_server_ingress.yaml
if [[ "${DEPLOY_KUBEFLOW}" -eq 1 ]]; then
	kubectl delete -f ${K8S_SETUP_SCRIPTS}/kfp_ingress.yaml
fi
}

usage(){
        cat <<EOF
"Usage: ./ingress.sh [deploy|cleanup]"
EOF
}

case "$op" in
	cleanup)
		header_text "Uninstalling Ingresses"
		delete
		;;
	deploy)
		header_text "Installing Ingresses"
		deploy
		;;
	*)
		usage
		;;
esac

