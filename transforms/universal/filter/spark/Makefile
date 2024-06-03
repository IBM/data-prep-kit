# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

# This is included in the image name, if defined
TRANSFORM_NAME=filter
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${FILTER_SPARK_VERSION}

venv::	.transforms.spark-venv

build::	.transforms.spark-build

test::	.transforms.spark-test

clean:: .transforms.clean

image:: .transforms.spark-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.spark-test-image

publish:: .transforms.spark-publish

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_spark.py \
		RUN_ARGS="--spark_local_config_filepath ../config/spark_profile_local.yml \
		--data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
		--filter_criteria_list \"[ 'docq_total_words > 100 AND docq_total_words < 200', 'ibmkenlm_docq_perplex_score < 230']\"  \
		--filter_columns_to_drop \"[ 'extra', 'cluster' ]\" "   \
		.transforms.run-src-file

run-local-python-only-sample: .transforms.run-local-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
