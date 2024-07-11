DOCKER_IMAGE_NAME=pyspark-base
DOCKER_IMAGE_VERSION=latest
DOCKER_FILE=Dockerfile
DOCKER_HOSTNAME=docker.io
DOCKER=docker
PYTHON=python

venv:	requirements.txt
	@# Help: Create the virtual environment using requirements.txt
	$(PYTHON) -m venv venv
	@source venv/bin/activate;		\
	pip install --upgrade pip;		\
	pip install wheel;			\
	if [ ! -z "$(EXTRA_INDEX_URL)" ]; then				\
		extra_url='--extra-index-url $(EXTRA_INDEX_URL)';	\
	fi;								\
	pip install $$extra_url -r requirements.txt;	

image: # Must be called with DOCKER_IMAGE_NAME=, DOCKER_IMAGE_VERSION= settings.
	@# Help: Create the docker image $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
	$(DOCKER) build -t $(DOCKER_REGISTRY_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) .

publish: # Must be called with DOCKER_IMAGE_NAME=, DOCKER_IMAGE_VERSION= settings.
	@# Help: Publish image $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) to $(DOCKER_HOSTNAME) container registry
	$(DOCKER) logout $(DOCKER_HOSTNAME)
	$(DOCKER) login $(DOCKER_HOSTNAME) -u '$(DOCKER_REGISTRY_USER)' -p '$(DOCKER_REGISTRY_KEY)'
	$(DOCKER) push  $(DOCKER_REGISTRY_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
