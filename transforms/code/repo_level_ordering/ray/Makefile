# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

include $(REPOROOT)/transforms/.make.transforms

BASE_IMAGE=$(RAY_BASE_IMAGE)

TRANSFORM_NAME=repo_level_order

venv::	.transforms.ray-venv

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.ray-test-image

build:: build-dist image

publish:: publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

# TRANSFORM_PYTHON_VERSION has no effect since requirements do not specify a python transform implementation
set-versions: 
	$(MAKE) TRANSFORM_PYTHON_VERSION=dummy TOML_VERSION=$(REPO_LVL_ORDER_RAY_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: .transforms.run-cli-ray-sample

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
