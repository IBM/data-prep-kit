REPOROOT=../../
# Use make help, to see the available rules

include ../../transforms/.make.transforms


DOCKER_IMAGE_NAME=ingest2parquet
TRANSFORM_NAME=ingest2parquet
DOCKER_IMAGE_VERSION=0.1

DOCKER_IMAGE_VERSION=0.1

venv::	.transforms.venv

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

test-image:: .transforms.test-image