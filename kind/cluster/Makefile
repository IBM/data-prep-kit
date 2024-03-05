
include Makefile.env

include hack/make-rules/cluster.mk

.PHONY: setup-cluster
setup-cluster:
	$(MAKE) kind
	$(MAKE) cluster-prepare
	$(MAKE) cluster-prepare-wait
	@echo "setup-cluster completed"

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
