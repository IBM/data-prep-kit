# Define the root of the local git clone for the common rules to be able
# # know where they are running from.
REPOROOT=../..

# Include the common rules.
# Use "make help" to see them.
include $(REPOROOT)/.make.defaults

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv

ifeq ($(KFPv2), 0)
DOCKER_FILE=Dockerfile
DOCKER_IMAGE_NAME=kfp-data-processing
DOCKER_IMAGE_VERSION=${KFP_DOCKER_VERSION}
KFP_SUPPORT_LIB=kfp_support_lib
else
DOCKER_FILE=Dockerfile_v2
DOCKER_IMAGE_NAME=kfp-data-processing_v2
DOCKER_IMAGE_VERSION=${KFP_DOCKER_VERSION_v2}
KFP_SUPPORT_LIB=kfp_support_lib_v2
endif


#DOCKER_IMG=${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}
DOCKER_IMG=$(DOCKER_LOCAL_IMAGE)


.PHONY: .lib-src-image
.lib-src-image:: 
	$(MAKE) .defaults.copy-lib LIB_PATH=$(DPK_RAY_LIB_DIR) LIB_NAME=data-processing-lib-ray
	$(MAKE) .defaults.copy-lib LIB_PATH=$(DPK_PYTHON_LIB_DIR) LIB_NAME=data-processing-lib-python
	$(MAKE) .defaults.copy-lib LIB_PATH=$(REPOROOT)/kfp/kfp_support_lib LIB_NAME=kfp_support_lib
	$(MAKE) .defaults.image
	rm -rf data-processing-lib-ray
	rm -rf data-processing-lib-python
	rm -rf kfp_support_lib

.PHONY: image
image: Dockerfile Dockerfile_v2 requirements.txt
	$(MAKE) reconcile-requirements
	$(MAKE) .lib-src-image

.PHONY: reconcile-requirements
reconcile-requirements:
ifeq ($(KFPv2), 1)
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION_v2}/" createRayClusterComponent.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION_v2}/" deleteRayClusterComponent.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION_v2}/" executeRayJobComponent.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION_v2}/" executeRayJobComponent_multi_s3.yaml
else
	@# Help: Update yaml files to build images tagged as version $(KFP_DOCKER_VERSION)
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" createRayClusterComponent.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" deleteRayClusterComponent.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" executeRayJobComponent.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" executeRayJobComponent_multi_s3.yaml
	sed -i.back "s/kfp-data-processing*:[0-9].*/$(DOCKER_IMAGE_NAME):${KFP_DOCKER_VERSION}/" executeSubWorkflowComponent.yaml
endif

.PHONY: load-image
load-image:
	@# Help: Load the image to the kind cluster created with make setup.
	kind load docker-image $(DOCKER_REMOTE_IMAGE) --name=$(KIND_CLUSTER_NAME)

.PHONY: build
build: image

.PHONY: publish
publish:
	$(MAKE) image .defaults.publish

test::

.PHONY: clean
clean:
	@# Help: Remove $(IMG) 
	-rm  makeenv
	rm -rf *.back || true
