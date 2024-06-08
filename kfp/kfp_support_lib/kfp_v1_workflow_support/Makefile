# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..
include ${REPOROOT}/.make.versions
include ${REPOROOT}/kfp/requirements.env

# Include the common rules.
# Use "make help" to see them.
include ${REPOROOT}/.make.defaults

# Command to run pytest
PYTHON_VERSION=$(shell $(PYTHON) --version)
VENV_ACTIVATE=venv/bin/activate

DEPLOY_KUBEFLOW ?= 0

clean::
	@# Help: Clean up the distribution build and the venv
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm -rf *.back || true


.check-env:: .check_python_version
	@echo "Checks passed"

set-versions:: .check-env
	@# Help: Copy the Makefile distribution version into the pyproject.toml
	sed -i.back 's/^version[ ]*=.*/version = "'${DPK_LIB_KFP_VERSION}'"/' pyproject.toml
	sed -i.back 's/data_prep_toolkit_ray==[0-9].*/data_prep_toolkit_ray==${DPK_LIB_VERSION}",/' pyproject.toml
	sed -i.back 's/kfp==[0-9].*/kfp==${KFP_v1}",/' pyproject.toml
	sed -i.back 's/ray=[0-9].*/ray==${RAY}",/' pyproject.toml

build:: set-versions venv
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make build` in upper directories and KFPv2==1
	echo "Skipping build as KFPv2 is defined"
else
	@# Help: Build the distribution for publishing to a pypi
	rm -r dist || true
	rm -rf src/*egg-info || true
	${PYTHON} -m pip install --upgrade build
	${PYTHON} -m build
endif

publish:: .check-env
	@# Help: Publish the wheel to testpypi
	if [ -d "dist"]; then rm -r dist; fi
	${PYTHON} -m pip install --upgrade build
	${PYTHON} -m twine check dist/*
	${PYTHON} -m twine upload --verbose --non-interactive dist/*

venv::	pyproject.toml .check-env
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make venv` in upper directories and KFPv2==1
	echo "Skipping as KFPv2 is defined"
else
	@# Help: Create the virtual environment using pyproject.toml
	rm -rf venv
	$(PYTHON) -m venv venv
	. ${VENV_ACTIVATE};     \
	cd ../../../data-processing-lib/python && make set-versions && cd -; \
	pip install -e ../../../data-processing-lib/python; \
	cd ../../../data-prepossesing-lib/ray && make set-versions && cd -; \
	pip install -e ../../../data-processing-lib/ray; \
	cd ../python_apiserver_client && make set-versions && cd -; \
	pip install -e ../python_apiserver_client;      \
	pip install -e .;				\
	pip install pytest pytest-cov
endif

test:: venv 
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make test` in upper directories and KFPv2==1
	echo "Skipping test as KFPv2 is defined"
else
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
ifeq ($(DEPLOY_KUBEFLOW),1)
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) ray_remote_jobs_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) pipeline_utils_test.py;
endif
endif
