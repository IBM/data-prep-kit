# Use make help, to see the available rules
REPOROOT=../..
include $(REPOROOT)/.make.defaults
SPARK_VERSION=3.5.1
DOCKER_IMAGE_BASE_NAME=data-prep-kit-spark-$(SPARK_VERSION)
DOCKER_IMAGE_LIB_NAME=data-prep-kit-spark
DOCKER_IMAGE_VERSION=$(DPK_LIB_VERSION)
TAG := "v${DPK_LIB_VERSION}"


clean::
	@# Help: Clean up the distribution build and the venv 
	rm -rf dist venv
	rm -rf src/*egg-info

.check-env::
	@echo "Checks passed"

update-toml:: .check-env
	@# Help: Copy the Makefile distribution version into the pyproject.toml 
	sed -e 's/^version[ ]*=.*/version = "'${DPK_LIB_VERSION}'"/' 	\
	    -e 's/"pyspark...*",/"pyspark>=${SPARK_VERSION}",/'		\
	    pyproject.toml > tt.toml
	mv tt.toml pyproject.toml

setup::

build:: update-toml venv
	@# Help: Build the distribution for publishing to a pypi 
	rm -r dist || true
	rm -rf src/*egg-info || true
	${PIP} install --upgrade build
	${PYTHON} -m build

publish:: .check-env update-toml
	@# Help: Publish project to pypi
	${PYTHON} -m twine check dist/*
	${PYTHON} -m twine upload --verbose --non-interactive dist/*
	#@echo "create a git tag to reference published version"
	#@git tag ${TAG}
	#@git push origin ${TAG}

venv::	pyproject.toml
	@# Help: Create the virtual environment using pyproject.toml 
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm makeenv || true
	$(PYTHON) -m venv venv
	source venv/bin/activate; 	\
	pip install --upgrade pip;	\
	pip install -e ../ray;		\
	pip install -e .;		\
	pip install pytest pytest-cov 

image:: image-spark

image-spark:: Dockerfile
	@# Help: Build the bare Spark image 
	$(MAKE) DOCKER_NAME=$(DOCKER_IMAGE_BASE_NAME) .defaults.image

#image-dpk:: Dockerfile
#	@# Help: Build the Spark image containing the data-prep-kit for spark 
#	$(MAKE) LIB_PATH=$(DPK_RAY_LIB_DIR) LIB_NAME=data-processing-lib .defaults.copy-lib
#	$(MAKE) DOCKER_NAME=$(DOCKER_IMAGE_LIB_NAME) .defaults.image
#	rm -rf data-processing-lib

# Here we run each test directory of tests and each ray launched test separately, because
# it seems when running multiple ray launch tests in a single pytest run there is some sort of ray.init() duplication.
# pytest-forked was tried, but then we get SIGABRT in pytest when running the s3 tests, some of which are skipped.. 
test::  
	@# Help: Use the already-built virtual environment to run pytest on the test directory. 
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/data_access;
	source venv/bin/activate; export PYTHONPATH=../src;  cd test; $(PYTEST)  data_processing_tests/transform;
	source venv/bin/activate; export PYTHONPATH=../src;  cd test; $(PYTEST)  data_processing_tests/launch/pure_python/launcher_test.py;
	source venv/bin/activate; export PYTHONPATH=../src;  cd test; $(PYTEST)  data_processing_tests/launch/pure_python/test_noop_launch.py;
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/launch/ray/ray_util_test.py;
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/launch/ray/launcher_test.py;
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/launch/ray/test_noop_launch.py;
