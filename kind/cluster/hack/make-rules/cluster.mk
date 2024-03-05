export K8S_VERSION:=${K8S_VERSION}
export K8S_CLUSTER:=${K8S_CLUSTER}
export KUBECONFIG:=${KUBECONFIG}

.PHONY: kind-setup
kind-setup:
	cd $(TOOLS_DIR); ./create_kind.sh

.PHONY: kind-cleanup
kind-cleanup:
	cd $(TOOLS_DIR); ./create_kind.sh cleanup

.PHONY: kind
kind: kind-cleanup kind-setup

