DOCKER=podman
DOCKER_FILE=Dockerfile
DOCKER_IMAGE_NAME=noop
DOCKER_IMAGE_VERSION=0.6
DOCKER_REGISTRY=docker-na.artifactory.swg-devops.com/wcp-ai-foundation-team-docker-virtual

PYTHON=python
PIP=$(PYTHON) -m pip


#requirements.txt: requirements-template.txt
#	user=$$(echo $(ARTIFACTORY_USER) | sed -e 's/@/%40/');	\
#	cat $? | sed -e "s/ARTIFACTORY_USER/$$user/" -e "s/ARTIFACTORY_API_KEY/$(ARTIFACTORY_API_KEY)/" > $@


clean: 
	rm -rf venv

# Create the local virtual environment, assuming python is already installed and available
# We upgrade pip as that seems to be required by watson_nlp
venv:	requirements.txt
	make check_env
	$(PYTHON) -m venv venv
	echo '[global]' > venv/pip.conf
	echo "extra-index-url = https://$$ARTIFACTORY_USER:$$ARTIFACTORY_API_KEY@na.artifactory.swg-devops.com/artifactory/api/pypi/res-data-engineering-team-pypi-local/simple" >> venv/pip.conf
	. venv/bin/activate;			\
	$(PIP) install --upgrade pip;		\
	$(PIP) install -r requirements.txt;	\
	$(PIP) install pytest;			\

check_env:
	@if [ -z "$(ARTIFACTORY_USER)" ]; then \
		echo "You must set ARTIFACTORY_USER and ARTIFACTORY_API_KEY env vars";\
		exit;									\
	fi

# Create the docker image making sure the preloaded models are available to copy into the image
image: requirements.txt 
	make check_env
	$(DOCKER) login $(DOCKER_REGISTRY) -u $(ARTIFACTORY_USER) -p $(ARTIFACTORY_API_KEY)
	$(DOCKER) build --build-arg ARTIFACTORY_USER=$(ARTIFACTORY_USER) --build-arg ARTIFACTORY_API_KEY=$(ARTIFACTORY_API_KEY) \
		-f $(DOCKER_FILE) -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) .


