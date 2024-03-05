#!/usr/bin/env bash

op=$1

source ${ROOT_DIR}/hack/common.sh

SLEEP_TIME="${SLEEP_TIME:-30}"
MAX_RETRIES="${MAX_RETRIES:-5}"
DEPLOYMENT_TIMEOUT="${DEPLOYMENT_TIMEOUT:-200}"
EXIT_CODE=0
TEKTON_KFP_SERVER_VERSION=1.8.1

wget https://raw.githubusercontent.com/kubeflow/kfp-tekton/v${TEKTON_KFP_SERVER_VERSION}/scripts/deploy/iks/helper-functions.sh -O /tmp/helper-functions.sh
source /tmp/helper-functions.sh
rm /tmp/helper-functions.sh

deploy_kuberay() {
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/
	helm repo update
	helm install kuberay-operator kuberay/kuberay-operator -n kuberay --version ${KUBERAY} --set image.pullPolicy=IfNotPresent --create-namespace
	helm install -f ${ROOT_DIR}/cluster/api_server_values.yaml kuberay-apiserver kuberay/kuberay-apiserver -n kuberay --set image.pullPolicy=IfNotPresent
	echo "Finished KubeRay deployment."
}

wait_kuberay(){
	echo "Wait for kuberay deployment."
	wait_for_pods "kuberay" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?

	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "KubeRay Deployment unsuccessful. Not all pods running"
		exit $EXIT_CODE
	fi
}

delete_kuberay(){
	helm uninstall kuberay-operator -n kuberay
}

case "$op" in
	cleanup)
		header_text "Uninstalling KubeRay"
		delete_kuberay
		;;
	deploy-wait)
		header_text "wait for KubeRay deployment"
		wait_kuberay
		;;
	*)
		header_text "Installing KubeRay"
		deploy_kuberay
		;;
esac

