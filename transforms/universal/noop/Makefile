#IBM_CLOUD_USER=
#IBM_CLOUD_API_KEY=
DOCKER_IMAGE_NAME=noop
DEPLOYMENT_YML=deployments/preprocessing/$(DOCKER_IMAGE_NAME).yml
DOCKER=docker
DOCKER_IMAGE_VERSION=0.5
DOCKER_FILE=Dockerfile
PYTHON=python
#DOCKER_REGISTRY=wcp-ai-foundation-team-docker-virtual.artifactory.swg-devops.com
#DOCKER_REGISTRY=docker-na-public.artifactory.swg-devops.com/wcp-ai-foundation-team-docker-virtual
DOCKER_REGISTRY=docker-na.artifactory.swg-devops.com/wcp-ai-foundation-team-docker-virtual
PIP=pip


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
	. venv/bin/activate;			    \
	$(PIP) install --upgrade pip;		\
	$(PIP) install -r requirements.txt;	\
	$(PIP) install pytest;

check_env:
	@if [ -z "$(ARTIFACTORY_USER)" ]; then \
		echo "You must set ARTIFACTORY_USER and ARTIFACTORY_API_KEY env vars";\
		exit;									\
	fi
	@if [ -z "$(IBM_CLOUD_USER)" ]; then \
		echo "You must set IBM_CLOUD_USER and IBM_CLOUD_API_KEY env vars";	\
		exit;									\
	fi

# Create the docker image making sure the preloaded models are available to copy into the image
image: requirements.txt 
	$(DOCKER) system prune -f 
	make check_env
	$(DOCKER) login $(DOCKER_REGISTRY) -u $(ARTIFACTORY_USER) -p $(ARTIFACTORY_API_KEY)
	$(DOCKER) build --build-arg ARTIFACTORY_USER=$(ARTIFACTORY_USER) --build-arg ARTIFACTORY_API_KEY=$(ARTIFACTORY_API_KEY) \
		-f $(DOCKER_FILE) -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) .

# Publish our built container to the ibm container registry
# Please run make docker first.
publish:
	make check_env
	ibmcloud login -q -u "$(IBM_CLOUD_USER)" -apikey "$(IBM_CLOUD_API_KEY)" 
	#ibmcloud target -g fmc-preprocessing 
	ibmcloud cr login --client docker
	$(DOCKER) tag $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) us.icr.io/cil15-shared-registry/preprocessing-pipelines/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
	$(DOCKER) push us.icr.io/cil15-shared-registry/preprocessing-pipelines/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
	#ibmcloud cr image-list | grep $(DOCKER_IMAGE_NAME)

redeploy: undeploy
	sleep 1
	oc apply -f  $(DEPLOYMENT_YML)
	sleep 5
	oc get pods | grep $(DOCKER_IMAGE_NAME) 

undeploy:
	-oc delete -f $(DEPLOYMENT_YML) 

