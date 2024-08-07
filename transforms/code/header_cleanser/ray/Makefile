
# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../../..
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=header_cleanser

BASE_IMAGE=${RAY_BASE_IMAGE}

clean:: .transforms.clean

test:: .transforms.ray-test

image:: .transforms.ray-image

venv:: .transforms.ray-venv

test-src:: .transforms.test-src

test-image:: .transforms.ray-test-image

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

set-versions: 
	$(MAKE) TRANSFORM_PYTHON_VERSION=${HEADER_CLEANSER_PYTHON_VERSION} TOML_VERSION=$(HEADER_CLEANSER_RAY_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --header_cleanser_contents_column_name \"contents\"	\
		--header_cleanser_license \"True\" "	\
                .transforms.run-src-file

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image
