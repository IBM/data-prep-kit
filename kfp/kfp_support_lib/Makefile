# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../..

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")
IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/\$\([A-Z_]*\)\-\([a-z]*\)/{\1\}\-\2/g' | sed 's/=/:=/' | sed 's/^/export /' >> makeenv")
include makeenv

# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

LIBNAME=fm_data_processing_kfp
PYTHON=python
PYTEST=pytest
VENV_ACTIVATE=venv/bin/activate

clean::
	@# Help: Clean up the distribution build and the venv 
	rm -r dist venv
	rm -rf src/*egg-info
	rm makeenv

.check-env::
	@if [ -z "$(FM_DATA_PROCESSING_KFP)" ]; then echo "You must provide FM_DATA_PROCESSING_KFP"; false; fi
	@echo "Checks passed"

update-toml:: .check-env
	@# Help: Copy the Makefile distribution version into the pyproject.toml 
	sed -e 's/^version[ ]*=.*/version = "'${FM_DATA_PROCESSING_KFP}'"/' pyproject.toml > tt.toml
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
	. ${VENV_ACTIVATE}; 	\
	pip install -e .;		\
	pip install ray==${RAY} \
	pip install pytest pytest-cov 

test:: 	
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) api_params_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST)  kfp_utils_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) kuberay_api_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) pipeline_utils_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) ray_remote_jobs_test.py;
