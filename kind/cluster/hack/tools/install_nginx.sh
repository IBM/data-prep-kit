#!/usr/bin/env bash

op=$1

SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-10}"
EXIT_CODE=0

source ../common.sh

deploy() {
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
}

wait(){
	echo "Wait for nginx deployment."
	wait_for_pods "ingress-nginx" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?

	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "NGINX Deployment unsuccessful. Not all pods running"
		exit $EXIT_CODE
	fi
}

delete(){
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
		delete
		;;
	deploy-wait)
		header_text "wait for NGINX deployment"
		wait
		;;
	deploy)
		header_text "Installing NGINX"
		deploy
		;;
	*)
		usage
		;;
esac

