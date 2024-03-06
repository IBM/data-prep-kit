#!/usr/bin/env bash

op=$1

source ${ROOT_DIR}/hack/common.sh

SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-10}"
EXIT_CODE=0

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

usage(){
        cat <<EOF
"Usage: ./install_nginx.sh [cleanup|deploy-wait|deploy]"
EOF
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
	deploy)
		header_text "Installing NGINX"
		deploy_nginx
		;;
	*)
		usage
		;;
esac

