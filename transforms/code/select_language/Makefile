
# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them.
include ../../.make.transforms

DOCKER_IMAGE_NAME=select-langauge
DOCKER_IMAGE_VERSION=0.1.0

clean: .transforms.clean

test:  .transforms.test

image: .transforms.image

build: .transforms.build

venv:  .transforms.venv

test-lib-src:: .transforms.test-lib-src

install-lib-src:: .transforms.install-lib-src
