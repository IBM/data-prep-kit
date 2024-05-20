# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=tokenization
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${TOKENIZER_VERSION}


venv::	.transforms.venv

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.test-image

publish:: .transforms.publish

run-cli-ray-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/ds01/input', 'output_folder' : '../output'}\"  \
                "  .transforms.run-src-file


run-local-sample: #.transforms.run-local-sample
	@# Help: Not implemented yet
	@echo "No src/tokenization_local.py yet :("

run-local-ray-sample: .transforms.run-local-ray-sample

run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image

workflow-venv::
	$(MAKE) -C kfp-workflow workflow-venv

workflow-build:: 
	$(MAKE) -C kfp-workflow workflow-build

workflow-test:: 
	$(MAKE) -C kfp-workflow workflow-test

.PHONY: workflow-upload
workflow-upload:
	$(MAKE) -C kfp-workflow workflow-upload
