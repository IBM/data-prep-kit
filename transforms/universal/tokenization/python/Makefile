# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=tokenization
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${TOKENIZATION_PYTHON_VERSION}

venv::	.transforms.python-venv

test::	.transforms.python-test

clean:: .transforms.clean

image:: .transforms.python-image

test-src:: .transforms.test-src

setup:: .transforms.setup

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=${TOKENIZATION_PYTHON_VERSION} TOML_VERSION=$(DOCKER_IMAGE_VERSION) .transforms.set-versions 
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

test-image:: .transforms.python-test-image

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_python.py \
                RUN_ARGS="--data_local_config \"{ 'input_folder' : '../test-data/ds01/input', 'output_folder' : '../output'}\"  \
                "  .transforms.run-src-file

run-local-sample: .transforms.run-local-python-sample

#run-s3-sample: .transforms.run-s3-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
