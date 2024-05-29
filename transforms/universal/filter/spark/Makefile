# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# This is included in the image name, if defined
TRANSFORM_RUNTIME=spark

include $(REPOROOT)/transforms/.make.transforms
TRANSFORM_NAME=filter
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${FILTER_SPARK_VERSION}

venv::	.transforms.spark-venv

build::	.transforms.spark-build

test::	.transforms.spark-test

clean:: .transforms.clean

image:: .transforms.spark-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.spark-test-image

publish:: .transforms.spark-publish

run-cli-sample: .transforms.run-cli-spark-sample

run-local-sample: .transforms.run-local-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
