# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=noop
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${NOOP_VERSION}

venv::	.transforms.ray-venv

build::	.transforms.ray-build

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.ray-test-image

publish:: .transforms.ray-publish

run-cli-ray-sample: .transforms.run-cli-ray-sample

run-local-sample: .transforms.run-local-sample

run-local-ray-sample: .transforms.run-local-ray-sample

run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image

.PHONY: workflow-venv
workflow-venv:
	$(MAKE) -C kfp-workflow workflow-venv

.PHONY: workflow-build
workflow-build:
	$(MAKE) -C kfp-workflow workflow-build

.PHONY: workflow-test
workflow-test:
	$(MAKE) -C kfp-workflow workflow-test

.PHONY: workflow-upload
workflow-upload:
	$(MAKE) -C kfp-workflow workflow-upload
