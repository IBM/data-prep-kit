# Define the root of the local git clone for the common rules to be able
# # know where they are running from.
REPOROOT=../..

# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv
DOCKER_FILE=Dockerfile
DOCKER_NAME ?= data-prep-lab/kfp-data-processing
IMG ?= ${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_NAME}:${KFP_DOCKER_TAGNAME}

# Create the docker image making sure the preloaded models are available to copy into the image
.kfp_comp.image:: Dockerfile requirements.txt
	$(call check_defined, DOCKER_HOSTNAME)
	sed -i.back "s/data-prep-lab-kfp==[0-9].*/data-prep-lab-kfp==${DATA_PREP_LAB_KFP}/" requirements.txt
	@# Help: Build the docker image using the $(DOCKER_FILE) and requirements.txt 
	$(DOCKER) build -t ${IMG} \
		--build-arg DATA_PREP_LAB=${DATA_PREP_LAB} \
		--build-arg DATA_PREP_LAB_KFP=${DATA_PREP_LAB_KFP} \
		--build-arg BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')  \
		--build-arg GIT_COMMIT=$(shell git log -1 --format=%h)  . --no-cache

image:: .kfp_comp.image
	make reconcile-requirements

reconcile-requirements::
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_TAGNAME}/" executeRayJobComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_TAGNAME}/" createRayComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_TAGNAME}/" cleanupRayComponent.yaml
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_TAGNAME}/" executeRayJobComponent_multi_s3.yaml

build:: image

publish::
	$(DOCKER) push ${IMG}

test::

clean::
	$(DOCKER) image rm ${IMG}
	rm makeenv
