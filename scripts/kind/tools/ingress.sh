#!/usr/bin/env bash

op=$1

source ${REPOROOT}/scripts/kind/common.sh

deploy() {
  kubectl apply -f ${REPOROOT}/scripts/kind/ray_api_server_ingress.yaml
if [[ "${DEPLOY_KUBEFLOW}" -eq 1 ]]; then
	kubectl apply -f ${REPOROOT}/scripts/kind/kfp_ingress.yaml
fi
}

delete(){
  kubectl delete -f ${REPOROOT}/scripts/kind/ray_api_server_ingress.yaml
if [[ "${DEPLOY_KUBEFLOW}" -eq 1 ]]; then
	kubectl delete -f ${REPOROOT}/scripts/kind/kfp_ingress.yaml
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

