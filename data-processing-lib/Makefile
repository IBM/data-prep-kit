# Use make help, to see the available rules
include ../.make.defaults

VERSION=0.1.0
TAG := "v${VERSION}"


clean::
	@# Help: Clean up the distribution build and the venv 
	rm -rf dist venv
	rm -rf src/*egg-info

.check-env::
	@if [ -z "$(VERSION)" ]; then echo "You must provide VERSION"; false; fi
	@echo "Checks passed"

update-toml:: .check-env
	@# Help: Copy the Makefile distribution version into the pyproject.toml 
	sed -e 's/^version[ ]*=.*/version = "'${VERSION}'"/' pyproject.toml > tt.toml
	mv tt.toml pyproject.toml

setup::

build:: update-toml venv
	@# Help: Build the distribution for publishing to a pypi 
	${PIP} install --upgrade build
	if [ -d "dist"]; then rm -r dist; fi
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
	rm -rf venv	
	$(PYTHON) -m venv venv
	source venv/bin/activate; 	\
	pip install --upgrade pip;	\
	pip install -e .;		\
	pip install pytest pytest-cov moto==5.0.5


# Here we run each test directory of tests and each ray launched test separately, because
# it seems when running multiple ray launch tests in a single pytest run there is some sort of ray.init() duplication.
# pytest-forked was tried, but then we get SIGABRT in pytest when running the s3 tests, some of which are skipped.. 
test::  
	@# Help: Use the already-built virtual environment to run pytest on the test directory. 
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/data_access;	
	source venv/bin/activate; export PYTHONPATH=../src;  cd test; $(PYTEST)  data_processing_tests/transform;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/ray/ray_util_test.py;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/ray/launcher_test.py;	
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_tests/ray/test_noop_launch.py;	

