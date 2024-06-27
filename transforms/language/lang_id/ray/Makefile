# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=lang_id
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${LANG_ID_RAY_VERSION}

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

setup:: .transforms.setup

# set the version of python transform that this depends on.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(LANG_ID_PYTHON_VERSION) .transforms.set-versions	
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS=" --run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --lang_id_model_credential \"ANY CREDENTIAL\"	\
				--lang_id_model_kind \"fasttext\"	\
				--lang_id_model_url \"facebook/fasttext-language-identification\" \
				--lang_id_content_column_name \"text\" "	\
                .transforms.run-src-file

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
