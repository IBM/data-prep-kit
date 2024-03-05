#!/usr/bin/env bash
set -x
op=$1

source ${ROOT_DIR}/hack/common.sh

SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-10}"
DEPLOYMENT_TIMEOUT="${DEPLOYMENT_TIMEOUT:-200}"
EXIT_CODE=0
TEKTON_KFP_SERVER_VERSION=1.8.1

wget https://raw.githubusercontent.com/kubeflow/kfp-tekton/v${TEKTON_KFP_SERVER_VERSION}/scripts/deploy/iks/helper-functions.sh -O /tmp/helper-functions.sh
source /tmp/helper-functions.sh
rm /tmp/helper-functions.sh

deploy_nginx() {
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
}

wait_nginx(){
	echo "Wait for nginx deployment."
	wait_for_pods "ingress-nginx" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?

	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "NGINX Deployment unsuccessful. Not all pods running"
		exit $EXIT_CODE
	fi
}

delete_nginx(){
	kubectl delete -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
}

case "$op" in
	cleanup)
		header_text "Uninstalling NGINX"
		delete_nginx
		;;
	deploy-wait)
		header_text "wait for NGINX deployment"
		wait_nginx
		;;
	*)
		header_text "Installing NGINX"
		deploy_nginx
		;;
esac

