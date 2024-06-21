# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms
include $(REPOROOT)/transforms/language/doc_quality/.make.doc_quality

TRANSFORM_NAME=doc_quality
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${DOC_QUALITY_RAY_VERSION}
DOCKER_HOME_DIR=/home/ray

venv::	.transforms.ray-venv

test:: lm_sp .transforms.ray-test

clean:: .transforms.clean clean_lm_sp

image:: lm_sp .transforms.ray-image

test-src:: lm_sp .transforms.test-src

setup:: .transforms.setup
	@which -s brew
	@if [[ $? == 0 ]]; then \
		brew install wget; \
	fi

test-image:: lm_sp .transforms.ray-image .transforms.test-image-help test-image-pytest 

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

# set the version of python transform that this depends on.
set-versions: 
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(DOC_QUALITY_RAY_VERSION) .transforms.set-versions 
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: lm_sp 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS=" --run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --docq_text_lang \"en\"	\
				--docq_doc_content_column \"contents\"	\
				--docq_bad_word_filepath \"../ldnoobw/en\" \
				--docq_kenLM_model \"../lm_sp/\" "	\
                .transforms.run-src-file

run-local-sample: lm_sp .transforms.run-local-ray-sample

run-s3-sample: lm_sp .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
