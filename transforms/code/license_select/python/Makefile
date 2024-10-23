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

DOCKER_IMAGE_VERSION=${LICENSE_SELECT_PYTHON_VERSION}

# Use default rule inherited from makefile.common
clean:: .transforms.clean

# Use default rule inherited from makefile.common
test:: .transforms.python-test

# Use default rule inherited from makefile.common
image:: .transforms.python-image

# Use default rule inherited from makefile.common
venv:: .transforms.python-venv

test-src:: .transforms.test-src

test-image:: .transforms.python-test-image

run-cli-sample:
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_python.py \
                RUN_ARGS="--data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
		 --lc_license_column_name license							\
    	 --lc_licenses_file  ../test-data/sample_approved_licenses.json" 					\
		.transforms.run-src-file

run-local-sample: .transforms.run-local-sample

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=${LICENSE_SELECT_PYTHON_VERSION} TOML_VERSION=$(LICENSE_SELECT_PYTHON_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

setup:: .transforms.setup
run-local-sample: .transforms.run-local-sample
run-local-python-sample: .transforms.run-local-python-sample

load-image:: .transforms.load-image

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image
