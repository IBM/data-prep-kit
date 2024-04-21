REPOROOT=../../
# Use make help, to see the available rules

include $(REPOROOT)/.make.defaults

DOCKER_IMAGE_NAME=ingest2parquet
DOCKER_IMAGE_VERSION=0.1
DOCKER_IMAGE=${DOCKER_REGISTRY_ENDPOINT}/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)


venv:	.defaults.lib-src-venv

build:	 venv image

test: test-src test-image	
	@# Help: Test both the source and the docker image

clean: .defaults.clean

image: .defaults.lib-src-image

test-src: .defaults.test-src

test-image: image .defaults.test-image-pytest
