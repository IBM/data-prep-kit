# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms

TRANSFORM_NAME=doc_id
DOCKER_IMAGE_VERSION=${DOC_ID_VERSION}

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
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --doc_id_int True "	\
                .transforms.run-src-file

run-local-sample: .transforms.run-local-sample

run-local-ray-sample: .transforms.run-local-ray-sample

run-s3-ray-sample: .transforms.run-s3-ray-sample

load-image:: .transforms.load-image
