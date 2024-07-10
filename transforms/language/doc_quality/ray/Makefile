# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/language/doc_quality/.make.doc_quality
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=doc_quality
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_HOME_DIR=/home/ray

venv::	.transforms.ray-venv

test:: download_model .transforms.test-src test-image

clean:: .transforms.clean clean_model

image:: download_model .transforms.ray-image

test-src:: download_model .transforms.test-src

setup:: .transforms.setup

test-image:: download_model .transforms.ray-image .transforms.test-image-help test-image-pytest 

build:: build-dist image

publish:: publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

# set the version of python transform that this depends on.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(DOC_QUALITY_PYTHON_VERSION) TOML_VERSION=$(DOC_QUALITY_PYTHON_VERSION) .transforms.set-versions	
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: download_model 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS=" --run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --docq_text_lang \"en\"	\
				--docq_doc_content_column \"contents\"	\
				--docq_bad_word_filepath \"../ldnoobw/en\" \
				--docq_model_path \"../../$(MODEL_DIR)/\"	\
				--docq_model_module_name \"perplexity_transform_model\"	\
				--docq_perplex_score_digit \"1\"	\
                .transforms.run-src-file

run-local-sample: download_model .transforms.run-local-ray-sample

run-s3-sample: download_model .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
