# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..

# Set this, before including .make.defaults, to 
#   1 if requirements reference the latest code in the data processing library 
#     in this repo (that is not yet published to pypi).	 This is the default setting.
#   0 if the transforms DPK dependencies are on wheels published to 
#     pypi (e.g. data-prep-toolkit=0.2.1)
#USE_REPO_LIB_SRC=1

# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

# Include the common configuration for this transform
include ../transform.config

BASE_IMAGE=$(RAY_BASE_IMAGE)

DOCKER_IMAGE_VERSION=${LICENSE_SELECT_RAY_VERSION}

# Use default rule inherited from makefile.common
clean:: .transforms.clean

# Use default rule inherited from makefile.common
test:: .transforms.ray-test

# Use default rule inherited from makefile.common
image:: .transforms.ray-image

# Use default rule inherited from makefile.common
venv:: .transforms.ray-venv

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
	$(MAKE) TRANSFORM_PYTHON_VERSION=${LICENSE_SELECT_PYTHON_VERSION}  TOML_VERSION=$(LICENSE_SELECT_RAY_VERSION) .transforms.set-versions
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-ray-sample:.
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../../python/test-data/input', 'output_folder' : '../output'}\"  \
		                      --lc_license_column_name license							\
    	                    --lc_licenses_file ../test-data/sample_approved_licenses.json" 					\
	.transforms.run-src-file

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start
kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

load-image:: .transforms.load-image
