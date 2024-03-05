export K8S_VERSION:=${K8S_VERSION}
export K8S_CLUSTER:=${K8S_CLUSTER}
export KUBECONFIG:=${KUBECONFIG}

.PHONY: create-kind-cluster
create-kind-cluster:
	cd $(TOOLS_DIR); ./kind_management.sh create_cluster

.PHONY: delete-kind-cluster
delete-kind-cluster:
	cd $(TOOLS_DIR); ./kind_management.sh delete_cluster

.PHONY: kind
kind: delete-kind-cluster create-kind-cluster

