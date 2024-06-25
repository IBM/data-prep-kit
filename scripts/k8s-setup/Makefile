# Define the root of the local git clone for the common rules to be able
# know where they are running from.
export REPOROOT=${CURDIR}/../../

# Include the common rules.
# Use "make help" to see them.
include ../.make.defaults

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kind/requirements.env > makeenv")
ifneq ($(KFPv2), 1)
	$(shell bash -c "echo PIPELINE_VERSION=$(KFP_V2)) >> makeenv
else
	$(shell bash -c "echo PIPELINE_VERSION=$(KFP_V1)) >> makeenv
endif
include makeenv

export TOOLS_DIR=${REPOROOT}/scripts/k8s-setup/tools
export KIND_SCRIPTS=${REPOROOT}/scripts/k8s-setup

export EXTERNAL_CLUSTER ?= 0
export DEPLOY_KUBEFLOW ?= 1
export USE_KFP_MINIO ?= 1
export POPULATE_TEST_DATA ?= 1

KIND_CLUSTER_NAME ?= dataprep

# The following taken from https://stackoverflow.com/a/65243296/45375
# Lists all targets and optional help text found in the target.
# :: rule means we first run the help from the main make.defaults.
extra-help:	
	@# Help: Shows additional make macros that can be overridden/used. 
	@echo ""
	@echo Overridable macro values include the following:
	@echo DOCKER - the name of the docker executable to use.  DOCKER=$(DOCKER)
	@echo DOCKER_FILE - the name of the docker file to use.  DOCKER_FILE=$(DOCKER_FILE)
	@echo DOCKER_REGISTRY_ENDPOINT - the docker registry location to publish images.  DOCKER_REGISTRY_ENDPOINT=$(DOCKER_REGISTRY_ENDPOINT)
	@echo DOCKER_HOSTNAME - the name of the docker registry to use.  DOCKER_HOSTNAME=$(DOCKER_HOSTNAME)
	@echo DOCKER_NAMESPACE - the name space to use in the registry.  DOCKER_NAMESPACE=$(DOCKER_NAMESPACE)
	@echo DOCKER_IMAGE_NAME - the name under the name space where images are publishes.  DOCKER_IMAGE_NAME=$(DOCKER_IMAGE_NAME)
	@echo DOCKER_REGISTRY_USER - the docker user to use.  DOCKER_REGISTRY_USER=$(DOCKER_REGISTRY_USER)
	@echo DOCKER_REGISTRY_KEY - the docker user to use.  DOCKER_REGISTRY_KEY=secret
	@echo PYTHON - the python executable to use.  PYTHON=$(PYTHON)
	@echo "TRANSFORM_RUNTIME_SRC_FILE is the base name of the python source file containing the main() (e.g. noop_local_ray.py)"
	@echo ""

setup::
ifneq ($(EXTERNAL_CLUSTER), 1)
	@# Help: Building kind cluster with everything installed
	$(MAKE) .create-kind-cluster
endif
	$(MAKE) cluster-deploy
	@echo "setup-cluster completed"

.PHONY: publish
publish:

populate-data::
	@# Help: Populate test data in Minio
	cd ${KIND_SCRIPTS} && ./populate_minio.sh

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
