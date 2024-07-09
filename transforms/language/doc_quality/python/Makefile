# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

TRANSFORM_RUNTIME=python
TRANSFORM_NAME=doc_quality
DOCKER_IMAGE_VERSION=${DOC_QUALITY_PYTHON_VERSION}
DOCKER_HOME_DIR=/home/dpk

include $(REPOROOT)/transforms/.make.transforms
include $(REPOROOT)/transforms/language/doc_quality/.make.doc_quality

venv::	.transforms.python-venv

test:: download_model .transforms.python-test

clean:: .transforms.clean clean_model

image:: download_model .transforms.python-image

test-src:: download_model .transforms.test-src

setup:: .transforms.setup
	which wget
	if [[ $$? == 1 ]]; then \
		which brew; \
		if [[ $$? == 0 ]]; then \
			brew install wget; \
		fi \
	fi

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-python

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TOML_VERSION=$(DOCKER_IMAGE_VERSION) .defaults.update-toml
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

test-image:: download_model .transforms.python-image .transforms.test-image-help test-image-pytest 

run-cli-sample: download_model
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_python.py \
                RUN_ARGS=" --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --docq_text_lang \"en\"	\
				--docq_doc_content_column \"contents\"	\
				--docq_bad_word_filepath \"../ldnoobw/en\" \
				--docq_kenLM_model \"../../$(MODEL_DIR)/\" "	\
                .transforms.run-src-file

run-local-sample: download_model .transforms.run-local-sample

run-local-python-sample: download_model .transforms.run-local-python-sample

#run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
