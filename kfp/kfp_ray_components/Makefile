# Define the root of the local git clone for the common rules to be able
# # know where they are running from.
REPOROOT=../..

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv
DOCKER_FILE=Dockerfile
DOCKER_NAME=kfp-data-processing
DOCKER_IMAGE_VERSION=${KFP_DOCKER_VERSION}
DOCKER_IMG=${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_NAME}:${DOCKER_IMAGE_VERSION}

# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

.PHONY: .copy-lib
.copy-lib:
	rm -rf ${LIB_NAME}
	mkdir ${LIB_NAME}
	cp -p -R ${LIB_PATH}/src ${LIB_NAME}
	cp -p -R ${LIB_PATH}/pyproject.toml ${LIB_NAME}
	cp -p -R ${LIB_PATH}/README.md ${LIB_NAME}

.PHONY: .lib-src-image
.lib-src-image:: 
	$(MAKE) .copy-lib LIB_PATH=$(REPOROOT)/data-processing-lib/ray LIB_NAME=data-processing-lib
	$(MAKE) .copy-lib LIB_PATH=$(REPOROOT)/kfp/kfp_support_lib LIB_NAME=kfp_support_lib
	$(MAKE) DOCKER_IMAGE=$(DOCKER_IMAGE) .defaults.image
	rm -rf data-processing-lib
	rm -rf kfp_support_lib

.PHONY: image
image: Dockerfile requirements.txt
	$(MAKE) reconcile-requirements
	$(MAKE) .lib-src-image

.PHONY: reconcile-requirements
reconcile-requirements:
	@# Help: Update yaml files to build images tagged as version $(KFP_DOCKER_VERSION)
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" executeRayJobComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" createRayComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" cleanupRayComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" executeRayJobComponent_multi_s3.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" executeSubWorkflowComponent.yaml

.PHONY: load-image
load-image:
	@# Help: Load the image to the kind cluster created with make setup.
	kind load docker-image $(DOCKER_IMG) --name=$(KIND_CLUSTER_NAME)

.PHONY: build
build: image

.PHONY: publish
publish:
	$(MAKE) image .defaults.publish

test::

.PHONY: clean
clean:
	@# Help: Remove $(IMG) 
	$(DOCKER) image rm ${IMG} || true
	-rm  makeenv
