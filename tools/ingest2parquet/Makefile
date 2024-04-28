REPOROOT=../../
# Use make help, to see the available rules

include $(REPOROOT)/.make.defaults

include ../../transforms/.make.transforms
include ../../.make.versions

TRANSFORM_NAME=ingest2parquet
DOCKER_IMAGE_VERSION=${INGEST_TO_PARQUET_VERSION}


venv::	.transforms.venv

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.test-image

publish:: .transforms.publish
