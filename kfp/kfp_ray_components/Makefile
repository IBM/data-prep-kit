KFP_DOCKER_TAGNAME=0.0.1
FM_DATA_PROCESSING=0.2.0
FM_DATA_PROCESSING_KFP=0.0.3.dev12

DOCKER=docker
DOCKER_FILE=Dockerfile
DOCKER_HOSTNAME ?= us.icr.io
DOCKER_NAMESPACE ?= cil15-shared-registry/preprocessing-pipelines
DOCKER_NAME ?= kfp-oc
IMG ?= ${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_NAME}:${KFP_DOCKER_TAGNAME}

check_defined = \
        $(strip $(foreach 1,$1, \
        $(call __check_defined,$1,$(strip $(value 2)))))
__check_defined = \
        $(if $(value $1),, \
        $(error Undefined $1$(if $2, ($2))))

# Create the docker image making sure the preloaded models are available to copy into the image
.kfp_comp.image:: Dockerfile requirements.txt
	$(call check_defined, ARTIFACTORY_USER)
	$(call check_defined, ARTIFACTORY_API_KEY)
	@# Help: Build the docker image using the $(DOCKER_FILE) and requirements.txt 
	$(DOCKER) build -t ${IMG} --build-arg ARTIFACTORY_USER=${ARTIFACTORY_USER} \
	        --build-arg ARTIFACTORY_API_KEY=${ARTIFACTORY_API_KEY} \
		--build-arg FM_DATA_PROCESSING=${FM_DATA_PROCESSING} --build-arg FM_DATA_PROCESSING_KFP=${FM_DATA_PROCESSING_KFP} \
		--build-arg BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')  \
		--build-arg GIT_COMMIT=$(shell git log -1 --format=%h)  . --no-cache

image:: .kfp_comp.image

build:: .kfp_comp.image

publish::
	$(DOCKER) push ${IMG}

test::
	@echo "No tests are provided"

clean::
	$(DOCKER) image rm ${IMG}
