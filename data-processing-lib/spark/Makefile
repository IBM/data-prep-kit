# Use make help, to see the available rules
REPOROOT=../..
include $(REPOROOT)/.make.defaults
SPARK_VERSION=3.5.1
DOCKER_IMAGE_NAME=data-prep-kit-spark-$(SPARK_VERSION)
DOCKER_IMAGE_LIB_NAME=data-prep-kit-spark
TAG := "v${DPK_LIB_VERSION}"


clean:: .defaults.clean

.check-env::
	@echo "Checks passed"

setup::

set-versions: .check-env
	$(MAKE) TOML_VERSION=$(DPK_LIB_VERSION) .defaults.update-toml
	sed -e 's/"pyspark...*",/"pyspark>=${SPARK_VERSION}",/'				\
	    pyproject.toml > tt.toml
	mv tt.toml pyproject.toml

build:: build-dist 

#build:: update-toml .defaults.build-dist 
build-dist :: .defaults.build-dist 

publish:: publish-dist 

publish-dist :: .check-env .defaults.publish-dist 

publish-image:: .defaults.publish-image

venv::  pyproject.toml
	$(MAKE) .defaults.spark-lib-src-venv
	pip install pytest pytest-cov 

image:: image-spark

image-spark:: Dockerfile
	@# Help: Build the bare Spark image 
	$(MAKE) .defaults.image

docker-save-image: .defaults.docker-save-image

docker-load-image: .defaults.docker-load-image

#image-dpk:: Dockerfile
#	@# Help: Build the Spark image containing the data-prep-kit for spark 
#	$(MAKE) LIB_PATH=$(DPK_RAY_LIB_DIR) LIB_NAME=data-processing-lib .defaults.copy-lib
#	$(MAKE) DOCKER_NAME=$(DOCKER_IMAGE_LIB_NAME) .defaults.image
#	rm -rf data-processing-lib

# Here we run each test directory of tests and each ray launched test separately, because
# it seems when running multiple ray launch tests in a single pytest run there is some sort of ray.init() duplication.
# pytest-forked was tried, but then we get SIGABRT in pytest when running the s3 tests, some of which are skipped.. 
test::  venv
	@# Help: Use the already-built virtual environment to run pytest on the test directory. 
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_spark_tests/launch/spark/test_noop_launch.py;
