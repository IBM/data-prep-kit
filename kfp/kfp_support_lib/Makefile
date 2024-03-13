# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

VERSION=0.0.4
TAG := "v${VERSION}"
LIBNAME=fm_data_processing_kfp
PYTHON=python3.10

# Lib versions
RAY=2.9.1

clean::
	@# Help: Clean up the distribution build and the venv 
	rm -r dist venv
	rm -rf src/*egg-info

.check-env::
	@if [ -z "$(VERSION)" ]; then echo "You must provide VERSION"; false; fi
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
	@${PYTHON} -m twine upload --verbose --non-interactive --skip-existing --repository testpypi dist/*

venv::	pyproject.toml .check-env
	@# Help: Create the virtual environment using pyproject.toml 
	rm -rf venv	
	$(PYTHON) -m venv venv
	. venv/bin/activate; 	\
	pip install -e .;		\
	pip install ray==${RAY} \
	pip install pytest pytest-cov 

test:: 	
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
	. venv/bin/activate; export PYTHONPATH=../src; cd test; pytest api_params_test.py;
	. venv/bin/activate; export PYTHONPATH=../src; cd test; pytest  kfp_utils_test.py;
	. venv/bin/activate; export PYTHONPATH=../src; cd test; pytest kuberay_api_test.py;
	. venv/bin/activate; export PYTHONPATH=../src; cd test; pytest pipeline_utils_test.py;
	. venv/bin/activate; export PYTHONPATH=../src; cd test; pytest ray_remote_jobs_test.py;
