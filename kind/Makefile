# Define the root of the local git clone for the common rules to be able
# know where they are running from.
export REPOROOT=${CURDIR}/../

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kind/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv

export ROOT_DIR=${CURDIR}
# Include the common rules.
# Use "make help" to see them.
include ../.make.defaults

export TOOLS_DIR=${ROOT_DIR}/hack/tools

export EXTERNAL_CLUSTER ?= 0
export DEPLOY_KUBEFLOW ?= 1
export USE_KFP_MINIO ?= 1
export POPULATE_TEST_DATA ?= 1
KIND_CLUSTER_NAME ?= dataprep

setup::
ifneq ($(EXTERNAL_CLUSTER), 1)
	@# Help: Building kind cluster with everything installed
	$(MAKE) .create-kind-cluster
endif
	$(MAKE) cluster-deploy
	@echo "setup-cluster completed"

populate-data::
	@# Help: Populate test data in Minio
	cd hack && ./populate_minio.sh

cluster-deploy::
	@# Help: Deploy all required tools on existing cluster
	$(MAKE) .cluster-prepare
	$(MAKE) .cluster-prepare-wait
ifneq ($(EXTERNAL_CLUSTER), 1)
	cd $(TOOLS_DIR) && ./ingress.sh deploy
ifeq ($(DEPLOY_KUBEFLOW)$(USE_KFP_MINIO),11)
	cd $(TOOLS_DIR) && ./install_minio.sh deploy
	cd $(TOOLS_DIR) && ./install_minio.sh deploy-wait
ifeq ($(POPULATE_TEST_DATA), 1)
	$(MAKE) populate-data
endif
endif
endif

clean::
ifneq ($(EXTERNAL_CLUSTER), 1)
	@# Help: Deleting the kind cluster
	cd $(TOOLS_DIR); ./kind_management.sh delete_cluster ${KIND_CLUSTER_NAME}
else
	cd $(TOOLS_DIR) && ./install_kuberay.sh cleanup
	cd $(TOOLS_DIR) && ./install_kubeflow.sh cleanup
endif

build::

test::

.create-kind-cluster::
	cd $(TOOLS_DIR); ./kind_management.sh create_cluster ${KIND_CLUSTER_NAME}

.cluster-prepare::
ifneq ($(EXTERNAL_CLUSTER), 1)
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy
endif
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy
ifeq ($(DEPLOY_KUBEFLOW),1)
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy
endif

.cluster-prepare-wait::
ifneq ($(EXTERNAL_CLUSTER), 1)
	cd $(TOOLS_DIR) && ./install_nginx.sh deploy-wait
endif
	cd $(TOOLS_DIR) && ./install_kuberay.sh deploy-wait
ifeq ($(DEPLOY_KUBEFLOW),1)
	cd $(TOOLS_DIR) && ./install_kubeflow.sh deploy-wait
endif
