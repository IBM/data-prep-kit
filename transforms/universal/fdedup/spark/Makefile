DOCKER_IMAGE_NAME=pyspark-base
DOCKER_IMAGE_VERSION=latest
DOCKER_FILE=Dockerfile
REGISTRY_HOST=docker.io
REGISTRY_PATH=
DOCKER=docker
PYTHON=python

venv:	requirements.txt requirements_fuzzy_dedup.txt
	@# Help: Create the virtual environment using requirements.txt and requirements_fuzzy_dedup.txt
	$(PYTHON) -m venv venv
	@source venv/bin/activate;		\
	pip install --upgrade pip;		\
	pip install wheel;			\
	pip install -r requirements.txt;	\
	pip install -r requirements_fuzzy_dedup.txt;

image: # Must be called with DOCKER_IMAGE_NAME=, DOCKER_IMAGE_VERSION= settings.
	@# Help: Create the docker image $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
	$(DOCKER) build -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) -f $(DOCKER_FILE) .

publish-docker: # Must be called with DOCKER_IMAGE_NAME=, DOCKER_IMAGE_VERSION= settings.
	@# Help: Publish image $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) to $(REGISTRY_HOST) container registry
	$(DOCKER) logout $(REGISTRY_HOST)
	$(DOCKER) login $(REGISTRY_HOST) -u '$(DOCKER_REGISTRY_USER)' -p '$(DOCKER_REGISTRY_KEY)'
	$(DOCKER) push  $(REGISTRY_PATH)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)

publish-ibm:
	ibmcloud login -q -u "$(IBM_CLOUD_USER)" -apikey "$(IBM_CLOUD_API_KEY)"
	ibmcloud cr login --client docker
	$(DOCKER) tag $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) $(REGISTRY_HOST)/$(REGISTRY_PATH)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
	$(DOCKER) push $(REGISTRY_HOST)/$(REGISTRY_PATH)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
	# ibmcloud cr image-list | grep $(DOCKER_IMAGE_NAME)
