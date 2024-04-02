# Define the root of the local git clone for the common rules to be able
# know where they are running from.
export REPOROOT=${CURDIR}/../

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kind/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")
IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kind/requirements.env | sed 's/\$\([A-Z_]*\)\-\([a-z]*\)/{\1\}\-\2/g' | sed 's/=/:=/' | sed 's/^/export /' >> makeenv")

include makeenv

export ROOT_DIR=${CURDIR}
# Include the common rules.
# Use "make help" to see them.
include ../.make.defaults

export TOOLS_DIR=${ROOT_DIR}/hack/tools

export DEPLOY_KUBEFLOW ?= 1

setup::
	@# Help: Building kind cluster with everything installed
	$(MAKE) .create-kind-cluster
	$(MAKE) cluster-deploy
	@echo "setup-cluster completed"

cluster-deploy:
	@# Help: Deploy all required tools on existing cluster
	$(MAKE) .cluster-prepare
	$(MAKE) .cluster-prepare-wait
	cd hack && ./ingress.sh deploy

clean::
	@# Help: Deleting the kind cluster
	cd $(TOOLS_DIR); ./kind_management.sh delete_cluster

build::


test::

.create-kind-cluster::
	cd $(TOOLS_DIR); ./kind_management.sh create_cluster

.cluster-prepare::
ifeq ($(DEPLOY_KUBEFLOW),1)
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy
endif
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy

.cluster-prepare-wait::
ifeq ($(DEPLOY_KUBEFLOW),1)
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy-wait
endif
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy-wait
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy-wait
