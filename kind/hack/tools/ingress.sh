#!/usr/bin/env bash

op=$1

source ${ROOT_DIR}/hack/common.sh

deploy() {
  kubectl apply -f ${ROOT_DIR}/hack/ray_api_server_ingress.yaml
if [[ "${DEPLOY_KUBEFLOW}" -eq 1 ]]; then
	kubectl apply -f ${ROOT_DIR}/hack/kfp_ingress.yaml
fi
}

delete(){
  kubectl delete -f ${ROOT_DIR}/hack/ray_api_server_ingress.yaml
if [[ "${DEPLOY_KUBEFLOW}" -eq 1 ]]; then
	kubectl delete -f ${ROOT_DIR}/hack/kfp_ingress.yaml
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

