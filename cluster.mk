.PHONY: create-kind-cluster
create-kind-cluster:
	cd $(TOOLS_DIR); ./kind_management.sh create_cluster

.PHONY: delete-kind-cluster
delete-kind-cluster:
	cd $(TOOLS_DIR); ./kind_management.sh delete_cluster

.PHONY: cluster-prepare
cluster-prepare:
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy

.PHONY: cluster-prepare-wait
cluster-prepare-wait:
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy-wait
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy-wait
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy-wait

