#!/usr/bin/env bash

op=$1

source ../common.sh

SLEEP_TIME="${SLEEP_TIME:-30}"
MAX_RETRIES="${MAX_RETRIES:-5}"
EXIT_CODE=0

deploy() {
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/
	helm repo update
	helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version ${KUBERAY} --set image.pullPolicy=IfNotPresent --create-namespace
	helm install -f ${ROOT_DIR}/hack/ray_api_server_values.yaml kuberay-apiserver kuberay/kuberay-apiserver -n kuberay --version 1.1.0 --set image.pullPolicy=IfNotPresent
	echo "Finished KubeRay deployment."
}

wait(){
	echo "Wait for kuberay deployment."
	wait_for_pods "kuberay" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?

	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "KubeRay Deployment unsuccessful. Not all pods running"
		exit $EXIT_CODE
	fi
}

delete(){
	helm uninstall kuberay-operator -n kuberay
}

usage(){
        cat <<EOF
"Usage: ./install_kuberay.sh [cleanup|deploy-wait|deploy]"
EOF
}


case "$op" in
	cleanup)
		header_text "Uninstalling KubeRay"
		delete
		;;
	deploy-wait)
		header_text "wait for KubeRay deployment"
		wait
		;;
	deploy)
		header_text "Installing KubeRay"
		deploy
		;;
	*)
		usage
		;;
esac

