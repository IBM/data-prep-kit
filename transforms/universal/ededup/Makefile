# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms

# import the transforms versions which are located in requirements.env file.
IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/transforms/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")
include makeenv

TRANSFORM_NAME=ededup
DOCKER_IMAGE_VERSION=${EDEDUP}

venv::	.transforms.venv

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.test-image-help
	@echo WARNING: Skipping pytest version of this test until we have a test/test_ededup.py file.


publish:: .transforms.publish
