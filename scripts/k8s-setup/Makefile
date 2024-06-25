# Define the root of the local git clone for the common rules to be able
# know where they are running from.
export REPOROOT=${CURDIR}/../../

# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/scripts/k8s-setup/requirements.env | sed 's/=/:=/;s/^/export /'  > makeenv")
ifeq ($(KFPv2), 1)
   $(shell bash -c "echo export PIPELINE_VERSION:=$(KFP_v2) >> makeenv")
else
   $(shell bash -c "echo export PIPELINE_VERSION:=$(KFP_v1) >> makeenv")
endif
include makeenv

export TOOLS_DIR=${REPOROOT}/scripts/k8s-setup/tools
export K8S_SETUP_SCRIPTS=${REPOROOT}/scripts/k8s-setup

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
	@echo EXTERNAL_CLUSTER - if not 1 - create a KinD cluster.  EXTERNAL_CLUSTER=$(EXTERNAL_CLUSTER)
	@echo DEPLOY_KUBEFLOW - If 1 deploy kubeflow pipelines on the cluster. DEPLOY_KUBEFLOW=$(DEPLOY_KUBEFLOW)
	@echo USE_KFP_MINIO - If 1 and DEPLOY_KUBEFLOW is also 1 then use the KFP Minio server for S3 tests.  USE_KFP_MINIO=$(USE_KFP_MINIO)
	@echo POPULATE_TEST_DATA - If 1 then upload the test data into the MinIO Object Store, deployed as part of KFP.  POPULATE_TEST_DATA=$(POPULATE_TEST_DATA)
	@echo MINIO_SERVER - MinIO endpoint. Default value is "http://localhost:8090" which is the MinIO deployed as part of KFP.
	@echo MINIO_ACCESS_KEY - MinIO access key. Default value is the access key of the MinIO deployed as part of KFP.
	@echo MINIO_SECRET_KEY - MinIO secret key. Default value is the secret key of the MinIO deployed as part of KFP.
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
	cd ${K8S_SETUP_SCRIPTS} && ./populate_minio.sh

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
