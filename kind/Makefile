# Define the root of the local git clone for the common rules to be able
# know where they are running from.
export REPOROOT=${CURDIR}/../

export ROOT_DIR=${CURDIR}
# Include the common rules.
# Use "make help" to see them.
include ../.make.defaults

export TOOLS_DIR=${ROOT_DIR}/hack/tools

export PIPELINE_VERSION := 1.8.5
export KUBERAY := 1.0.0


setup-kind-cluster::
	@# Help: Building kind cluster with everything installed
	$(MAKE) .create-kind-cluster
	$(MAKE) .cluster-prepare
	$(MAKE) .cluster-prepare-wait
	cd hack && ./ingress.sh deploy
	@echo "setup-cluster completed"

delete-kind-cluster::
	@# Help: Deleting the kind cluster
	cd $(TOOLS_DIR); ./kind_management.sh delete_cluster

.create-kind-cluster::
	cd $(TOOLS_DIR); ./kind_management.sh create_cluster

.cluster-prepare::
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy

.cluster-prepare-wait::
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy-wait
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy-wait
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy-wait

