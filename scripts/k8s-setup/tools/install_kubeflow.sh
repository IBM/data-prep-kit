#!/usr/bin/env bash

op=$1

source ../common.sh

SLEEP_TIME="${SLEEP_TIME:-60}"
MAX_RETRIES="${MAX_RETRIES:-20}"
EXIT_CODE=0

deploy() {
	TEMP_DIR="$(mktemp -d)"
	echo "Temporary dir:"
	echo "${TEMP_DIR}"
	cd $TEMP_DIR
	git clone https://github.com/kubeflow/pipelines.git --branch ${PIPELINE_VERSION} --single-branch
	cd pipelines
	kubectl apply -k manifests/kustomize/cluster-scoped-resources
	kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
	# Disable the public endpoint
	# ref: https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#disable-the-public-endpoint
	sed -i.back '/inverse-proxy$/d' manifests/kustomize/env/dev/kustomization.yaml
	sed -i.back 's/30Mi/60Mi/' manifests/kustomize/third-party/application/application-controller-deployment.yaml
	sed -i.back 's/20Mi/60Mi/' manifests/kustomize/third-party/application/application-controller-deployment.yaml
	deploy_with_retries "-k" "manifests/kustomize/env/dev" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?
	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "Kubeflow deployment unsuccessful."
		exit 1
	fi
	echo "Finished Kubeflow deployment."
	rm -rf $TEMP_DIR
}

wait(){
	echo "Wait for kubeflow deployment."
	# see https://github.com/kubeflow/pipelines/issues/5411
	kubectl delete deployment -n kubeflow controller-manager
	wait_for_pods "kubeflow" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?

	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "Kubeflow Deployment unsuccessful. Not all pods running"
		exit $EXIT_CODE
	fi
	# disable cache for testing
	# ref https://www.kubeflow.org/docs/components/pipelines/v1/overview/caching/#disabling-caching-in-your-kubeflow-pipelines-deployment
	kubectl patch mutatingwebhookconfiguration cache-webhook-kubeflow --type='json' -p='[{"op":"replace", "path": "/webhooks/0/rules/0/operations/0", "value": "DELETE"}]'
}

delete(){
  kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=$PIPELINE_VERSION" --ignore-not-found || true
  kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION" --ignore-not-found || true
  kubectl delete --ignore-not-found clusterrolebinding pipeline-runner-extend
}

usage(){
        cat <<EOF
"Usage: ./install_kubeflow.sh [cleanup|deploy-wait|deploy]"
EOF
}

case "$op" in
	cleanup)
		header_text "Uninstalling Kubeflow"
		delete
		;;
	deploy-wait)
		header_text "wait for Kubeflow deployment"
		wait
		;;
	deploy)
		header_text "Installing Kubeflow"
		deploy
		;;
	*)
		usage
  	;;
esac

