#!/usr/bin/env bash

op=$1

source ../common.sh

SLEEP_TIME="${SLEEP_TIME:-50}"
MAX_RETRIES="${MAX_RETRIES:-20}"
EXIT_CODE=0

deploy() {
  kubectl apply -f ${K8S_SETUP_SCRIPTS}/s3_secret.yaml
	kubectl apply -f ${K8S_SETUP_SCRIPTS}/minio_ingress.yaml
}

wait(){
	echo "Wait for minio server"
	ns="kubeflow"
	while [[ $(kubectl get ingress minio -n $ns -o jsonpath="{.status.loadBalancer.ingress[0].hostname}") != "localhost" ]]; do
    echo "still waiting for minio ingress to get ready"
    sleep 20
  done
echo "ingress minio is ready"
}

delete(){
  kubectl delete -f ${K8S_SETUP_SCRIPTS}/s3_secret.yaml
	kubectl delete -f ${K8S_SETUP_SCRIPTS}/minio_ingress.yaml
}

usage(){
        cat <<EOF
"Usage: ./install_minio.sh [cleanup|deploy-wait|deploy]"
EOF
}

case "$op" in
	cleanup)
		header_text "Uninstalling Minio"
		delete
		;;
	deploy-wait)
		header_text "wait for Minio deployment"
		wait
		;;
	deploy)
		header_text "Installing Minio"
		deploy
		;;
	*)
		usage
  	;;
esac

