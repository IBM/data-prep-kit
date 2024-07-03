# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=profiler

BASE_IMAGE=${RAY_BASE_IMAGE}
venv::	.transforms.ray-venv

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup


# TRANSFORM_PYTHON_VERSION has no effect since requirements do not specify a python transform implementation
set-versions: 
	$(MAKE)  TRANSFORM_PYTHON_VERSION=dummy TOML_VERSION=$(PROFILER_RAY_VERSION) .transforms.set-versions 

test-image:: .transforms.ray-test-image

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

build-dist:: .defaults.build-dist

publish-dist:: .defaults.publish-dist

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image

run-cli-sample:
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --profiler_num_aggregators 2"	\
                .transforms.run-src-file
