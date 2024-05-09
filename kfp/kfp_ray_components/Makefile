# Define the root of the local git clone for the common rules to be able
# # know where they are running from.
REPOROOT=../..

# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv
DOCKER_FILE=Dockerfile
DOCKER_NAME=kfp-data-processing
IMG=${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_NAME}:${KFP_DOCKER_VERSION}

# Create the docker image making sure the preloaded models are available to copy into the image
.kfp_comp.image:: Dockerfile requirements.txt
	@# Help: Build the docker image using the $(DOCKER_FILE) and requirements.txt 
	$(call check_defined, DOCKER_HOSTNAME)
	sed -i.back "s/data-prep-toolkit-kfp==[0-9].*/data-prep-toolkit-kfp==${DPK_LIB_KFP_VERSION}/" requirements.txt
	$(DOCKER) build -t ${IMG} \
		--build-arg BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')  \
		--build-arg GIT_COMMIT=$(shell git log -1 --format=%h)  . --no-cache

image:: .kfp_comp.image
	$(MAKE) reconcile-requirements

reconcile-requirements::
	@# Help: Update yaml files to build images tagged as version $(KFP_DOCKER_VERSION)
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" executeRayJobComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" createRayComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" cleanupRayComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" executeRayJobComponent_multi_s3.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_VERSION}/" executeSubWorkflowComponent.yaml

load-image:
	@# Help: Load the image to the kind cluster created with make setup.
	kind load docker-image $(IMG) --name=$(KIND_CLUSTER_NAME)

build:: image

publish::
	@# Help: Push $(IMG) to the registry 
	$(DOCKER) push ${IMG}

test::

clean::
	@# Help: Remove $(IMG) 
	$(DOCKER) image rm ${IMG} || true
	-rm  makeenv
