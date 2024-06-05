# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=ingest_2_parquet
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${INGEST_TO_PARQUET_VERSION}

venv::	.transforms.ray-venv

build::	.transforms.ray-build

test::	.transforms.ray-test

clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.ray-test-image

publish:: .transforms.ray-publish


run-cli-ray-sample: .transforms.run-cli-ray-sample
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform.py \
                RUN_ARGS="--run_locally True \
                --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --data_files_to_use \"['.zip']\"  \
                --runtime_num_workers 3 \
				--ingest_to_parquet_supported_langs_file ../test-data/languages/lang_extensions.json	\
    			--ingest_to_parquet_detect_programming_lang True	\
				--ingest_to_parquet_snapshot github	\
				--ingest_to_parquet_domain code " \
			    .transforms.run-src-file

run-local-sample: .transforms.run-local-sample

run-local-ray-sample: .transforms.run-local-ray-sample

run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
