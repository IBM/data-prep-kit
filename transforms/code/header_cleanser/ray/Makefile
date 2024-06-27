
# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../../..
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=header_cleanser
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${HEADER_CLEANSER_RAY_VERSION}

venv::	.transforms.ray-venv

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.ray-test-image

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-ray

set-versions: 
	$(MAKE) TRANSFORM_PYTHON_VERSION=${HEADER_CLEANSER_PYTHON_VERSION} TOML_VERSION=$(HEADER_CLEANSER_RAY_VERSION) .transforms.set-versions 
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --header_cleanser_contents_column_name \"contents\"	\
		--header_cleanser_license \"True\" "	\
                .transforms.run-src-file

run-local-python-only-sample: .transforms.run-local-sample

run-local-sample: .transforms.run-local-sample

run-s3-sample: .transforms.run-s3-sample

minio-start:	.minio-start

load-image:: .transforms.load-image