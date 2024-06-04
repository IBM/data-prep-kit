# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../..
include ${REPOROOT}/.make.versions
include ${REPOROOT}/kfp/requirements.env

# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

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
	$(MAKE) TOML_VERSION=$(DPK_LIB_KFP_VERSION) .defaults.update-toml
	sed -i.back 's/kfp==[0-9].*/kfp==${KFP}",/' pyproject.toml
	sed -i.back 's/ray==[0-9].*/ray==${RAY}",/' pyproject.toml

build-dist:: set-versions .defaults.build-dist

publish:: publish-dist

publish-dist:: .check-env .defaults.publish-dist

build:: build-dist 

venv::	pyproject.toml .check-env .defaults.venv
	$(MAKE) .defaults.install-python-lib-src-venv
	. ${VENV_ACTIVATE};     \
	pip install -e .;	\
	pip install pytest pytest-cov;
	@# Help: Create the virtual environment using pyproject.toml 

test:: 	venv
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) api_params_test.py;
ifeq ($(DEPLOY_KUBEFLOW),1)
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) kuberay_api_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) ray_remote_jobs_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) pipeline_utils_test.py;
endif
