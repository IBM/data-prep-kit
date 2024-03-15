# Use make help, to see the available rules
include ../.make.defaults

VERSION=0.2.0
TAG := "v${VERSION}"
LIBNAME=fm_data_processing
PYTHON=python
PYTEST=pytest
ARTIFACTORY_DESTINATION=https://na.artifactory.swg-devops.com/artifactory/api/pypi/res-data-engineering-team-pypi-local


clean::
	@# Help: Clean up the distribution build and the venv 
	rm -r dist venv
	rm -rf src/*egg-info

.check-env::
	@if [ -z "$(VERSION)" ]; then echo "You must provide VERSION"; false; fi
	@if [ -z "$(ARTIFACTORY_USER)" ] || [ -z "$(ARTIFACTORY_API_KEY)" ]; then echo "You must set ARTIFACTORY_USER and ARTIFACTORY_API_KEY env vars"; false ; fi
	@echo "Checks passed"

update-toml:: .check-env
	@# Help: Copy the Makefile distribution version into the pyproject.toml 
	sed -e 's/^version[ ]*=.*/version = "'${VERSION}'"/' pyproject.toml > tt.toml
	mv tt.toml pyproject.toml

build:: update-toml venv
	@# Help: Build the distribution for publishing to a pypi 
	${PYTHON} -m pip install --upgrade build
	${PYTHON} -m build

publish:: .check-env
	@# Help: Publish the wheel to and pypi 
	@twine upload --verbose --non-interactive --skip-existing \
		--repository-url ${ARTIFACTORY_DESTINATION}	\
		-u ${ARTIFACTORY_USER} \
		-p ${ARTIFACTORY_API_KEY} \
		dist/${LIBNAME}-${VERSION}-py3-none-any.whl
	#@echo "create a git tag to reference published version"
	#@git tag ${TAG}
	#@git push origin ${TAG}

venv::	pyproject.toml .check-env
	@# Help: Create the virtual environment using pyproject.toml 
	rm -rf venv	
	$(PYTHON) -m venv venv
	echo '[global]' > venv/pip.conf
	echo "extra-index-url = https://$$ARTIFACTORY_USER:$$ARTIFACTORY_API_KEY@na.artifactory.swg-devops.com/artifactory/api/pypi/res-data-engineering-team-pypi-local/simple" >> venv/pip.conf
	source venv/bin/activate; 	\
	pip install --upgrade pip;	\
	pip install -e .;		\
	pip install pytest pytest-cov 

# Here we run each test directory of tests and each ray launched test separately, because
# it seems when running multiple ray launch tests in a single pytest run there is some sort of ray.init() duplication.
# pytest-forked was tried, but then we get SIGABRT in pytest when running the s3 tests, some of which are skipped.. 
test::  
	@# Help: Use the already-built virtual environment to run pytest on the test directory. 
	source venv/bin/activate; export PYTHONPATH=../src;  cd test; $(PYTEST)  data_processing_tests/transform;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/data_access;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/ray/ray_util_test.py;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/ray/test_noop_launch.py;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/ray/launcher_test.py;	
