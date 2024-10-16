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

venv::	.transforms.spark-venv

test::	.transforms.spark-test

clean:: .transforms.clean

image:: .transforms.spark-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.spark-test-image

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-spark

set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=dummy TOML_VERSION=$(FILTER_SPARK_VERSION) .transforms.set-versions
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-cli-sample: 
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_spark.py \
		RUN_ARGS="--spark_local_config_filepath ../config/spark_profile_local.yml \
		--data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
		--filter_criteria_list \"[ 'docq_total_words > 100 AND docq_total_words < 200', 'ibmkenlm_docq_perplex_score < 230']\"  \
		--filter_columns_to_drop \"[ 'extra', 'cluster' ]\" "   \
		.transforms.run-src-file

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image
