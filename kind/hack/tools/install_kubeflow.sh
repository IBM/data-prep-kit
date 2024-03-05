#!/usr/bin/env bash

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

deploy_kubeflow() {
	cd /tmp
	git clone https://github.com/kubeflow/pipelines.git
	cd pipelines
	git checkout tags/${PIPELINE_VERSION}
	kubectl apply -k manifests/kustomize/cluster-scoped-resources
	kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
	# Disable the public endpoint
	# ref: https://www.kubeflow.org/docs/components/pipelines/v1/installation/standalone-deployment/#disable-the-public-endpoint
	sed -i '/inverse-proxy$/d' manifests/kustomize/env/dev/kustomization.yaml
	deploy_with_retries "-k" "manifests/kustomize/env/dev" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?
	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "Kfp-Tekton deployment unsuccessful."
		exit 1
	fi

    # FIXME: avoid using cluster-admin role
	kubectl create clusterrolebinding pipeline-runner-extend --clusterrole cluster-admin --serviceaccount=kubeflow:pipeline-runner
	echo "Finished Kubeflow deployment."
	rm -rf /tmp/pipelines
}

wait_kubeflow(){
	echo "Wait for kubeflow deployment."
	wait_for_pods "kubeflow" "$MAX_RETRIES" "$SLEEP_TIME" || EXIT_CODE=$?

	if [[ $EXIT_CODE -ne 0 ]]
	then
		echo "Kubeflow Deployment unsuccessful. Not all pods running"
		exit $EXIT_CODE
	fi
}

delete_kubeflow(){
	kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION"
	kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=$PIPELINE_VERSION"
}

case "$op" in
	cleanup)
		header_text "Uninstalling Kubeflow"
		delete_kubeflow
		;;
	deploy-wait)
		header_text "wait for Kubeflow deployment"
		wait_kubeflow
		;;
	*)
		header_text "Installing Kubeflow"
		deploy_kubeflow
		;;
esac

