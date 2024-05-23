# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=ededup
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${EDEDUP_VERSION}

venv::	.transforms.ray-venv

build::	.transforms.ray-build

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.test-image-help
	@echo WARNING: Skipping pytest version of this test until we have a test/test_ededup.py file.


publish:: .transforms.ray-publish

run-cli-ray-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --ededup_num_hashes 2"	\
                .transforms.run-src-file



run-local-sample: 
	@# Help: Not implemented yet
	@echo "No src/ededup_local.py yet :("

run-local-ray-sample: .transforms.run-local-ray-sample

run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image

.PHONY: workflow-venv
workflow-venv:
	$(MAKE) -C kfp_ray/v1 workflow-venv

.PHONY: workflow-build
workflow-build:
	$(MAKE) -C kfp_ray/v1 workflow-build

.PHONY: workflow-test
workflow-test:
	$(MAKE) -C kfp_ray/v1 workflow-test

.PHONY: workflow-upload
workflow-upload:
	$(MAKE) -C kfp_ray/v1 workflow-upload

.PHONY: workflow-reconcile-requirements
workflow-reconcile-requirements:
	$(MAKE) -C kfp_ray/v1 workflow-reconcile-requirements
