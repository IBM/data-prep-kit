# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=profiler
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${PROFILER_RAY_VERSION}

venv::	.transforms.ray-venv

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup


# TRANSFORM_PYTHON_VERSION has no effect since requirements do not specify a python transform implementation
set-versions: 
	$(MAKE) TRANSFORM_PYTHON_VERSION=${DOCKER_IMAGE_VERSION} .transforms.set-versions 

test-image:: .transforms.ray-test-image

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

build-dist:: .defaults.build-dist

publish-dist:: .defaults.publish-dist

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image

run-cli-sample:
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --profiler_num_aggregators 2"	\
                .transforms.run-src-file
