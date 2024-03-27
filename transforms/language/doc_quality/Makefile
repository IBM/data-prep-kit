
# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them. 
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below.
include ../../.make.transforms

DOCKER_IMAGE_NAME=doc_quality
DOCKER_IMAGE_VERSION=0.1.0


venv::	.transforms.venv

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-lib-src:: .transforms.test-lib-src

install-lib-src:: .transforms.install-lib-src


