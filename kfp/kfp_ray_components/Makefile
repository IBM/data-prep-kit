# Define the root of the local git clone for the common rules to be able
# # know where they are running from.
REPOROOT=../..

# Include the common rules.
# Use "make help" to see them.
include $(REPOROOT)/.make.defaults

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv

ifeq ($(KFPv2), 1)
DOCKER_IMAGE_NAME=kfp-data-processing_v2
DOCKER_IMAGE_VERSION=${KFP_DOCKER_VERSION_v2}
WORKFLOW_SUPPORT_LIB=kfp_v2_workflow_support
else
DOCKER_IMAGE_NAME=kfp-data-processing
DOCKER_IMAGE_VERSION=${KFP_DOCKER_VERSION}
WORKFLOW_SUPPORT_LIB=kfp_v1_workflow_support
endif


#DOCKER_IMG=${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}
DOCKER_IMG=$(DOCKER_LOCAL_IMAGE)

.PHONY: .lib-src-image
.lib-src-image:: 
	$(MAKE) .defaults.copy-lib LIB_PATH=$(DPK_RAY_LIB_DIR) LIB_NAME=data-processing-lib-ray
	$(MAKE) .defaults.copy-lib LIB_PATH=$(DPK_PYTHON_LIB_DIR) LIB_NAME=data-processing-lib-python
	$(MAKE) .defaults.copy-lib LIB_PATH=$(REPOROOT)/kfp/kfp_support_lib/shared_workflow_support LIB_NAME=shared_workflow_support_lib
	$(MAKE) .defaults.copy-lib LIB_PATH=$(REPOROOT)/kfp/kfp_support_lib/$(WORKFLOW_SUPPORT_LIB) LIB_NAME=workflow_support_lib
	$(MAKE) .defaults.image
	rm -rf data-processing-lib-ray
	rm -rf data-processing-lib-python
	rm -rf shared_workflow_support_lib
	rm -rf workflow_support_lib

.PHONY: image
image: Dockerfile requirements.txt
	$(MAKE) reconcile-requirements
	$(MAKE) .lib-src-image


set-versions:  reconcile-requirements

.PHONY: reconcile-requirements
reconcile-requirements:
	@# Help: Update yaml files to build images tagged as version $(KFP_DOCKER_VERSION)
	sed -i.back "s/kfp-data-processing.*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" createRayClusterComponent.yaml
	sed -i.back "s/kfp-data-processing.*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" deleteRayClusterComponent.yaml
	sed -i.back "s/kfp-data-processing.*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" executeRayJobComponent.yaml
	sed -i.back "s/kfp-data-processing.*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" executeRayJobComponent_multi_s3.yaml
	# TODO remove it for KFPv2
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" executeSubWorkflowComponent.yaml

.PHONY: load-image
load-image:
	@# Help: Load the image to the kind cluster created with make setup.
	kind load docker-image $(DOCKER_REMOTE_IMAGE) --name=$(KIND_CLUSTER_NAME)

.PHONY: build
build: image

.PHONY: publish
publish:
	$(MAKE) image .defaults.publish-image

test::

.PHONY: clean
clean:
	@# Help: Remove $(IMG) 
	-rm  makeenv
	rm -rf *.back || true
