# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

include $(REPOROOT)/transforms/.make.transforms

# Include the common configuration for this transform
include ../transform.config

# values possible mach-arm64, x86_64
export RUNTIME_HOST_ARCH=x86_64

BASE_IMAGE=${RAY_BASE_IMAGE}
venv::	.transforms.ray-venv

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.ray-test-image

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup
# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(CODE_PROFILER_RAY_VERSION) TOML_VERSION=$(CODE_PROFILER_RAY_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

# Ensure RUN_ARGS has a default value
RUN_ARGS ?= ""

run-cli-sample: .transforms.run-cli-ray-sample

run-local-sample: .transforms.run-local-ray-sample

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image
