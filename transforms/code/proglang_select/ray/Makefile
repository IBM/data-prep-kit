# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=proglang_select
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${PROGLANG_SELECT_VERSION}

venv::	.transforms.venv

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.test-image

publish:: .transforms.publish


run-cli-ray-sample: .transforms.run-cli-ray-sample
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
		--proglang_select_language_column language							\
    		--proglang_select_output_column lang_selected							\
		--proglang_select_allowed_langs_file ../test-data/languages/allowed-code-languages.txt "	\
		.transforms.run-src-file

run-local-sample: .transforms.run-local-sample

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
