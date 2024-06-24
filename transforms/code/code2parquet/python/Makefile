# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

TRANSFORM_RUNTIME=python
TRANSFORM_NAME=code2parquet
DOCKER_IMAGE_VERSION=${CODE2PARQUET_PYTHON_VERSION}

include $(REPOROOT)/transforms/.make.transforms

venv::	.transforms.python-venv

test::	.transforms.python-test

clean:: .transforms.clean

image:: .transforms.python-image

test-src:: .transforms.test-src

setup:: .transforms.setup

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TOML_VERSION=$(DOCKER_IMAGE_VERSION) .defaults.update-toml
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

test-image:: .transforms.python-test-image

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_python.py \
	    RUN_ARGS="	\
	    --data_local_config \" { 'input_folder' : '../test-data/input', 'output_folder' : '../output' } \"  \
	    --data_files_to_use \"['.zip']\"  \
	    --code2parquet_supported_langs_file ../test-data/languages/lang_extensions.json     \
	    --code2parquet_detect_programming_lang True "       \
	    .transforms.run-src-file

run-local-sample: .transforms.run-local-sample

run-local-python-sample: .transforms.run-local-python-sample

#run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
