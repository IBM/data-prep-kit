# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..

# Include the common rules.
# Use "make help" to see them.
include ${REPOROOT}/.make.defaults

# Command to run pytest
PYTHON_VERSION=$(shell $(PYTHON) --version)
VENV_ACTIVATE=venv/bin/activate

DEPLOY_KUBEFLOW ?= 0

.PHONY: clean
clean::
	@# Help: Clean up the distribution build and the venv
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm -rf *.back || true

.PHONY: .check-env
.check-env:: .check_python_version
	@echo "Checks passed"

.PHONY: set-versions
set-versions:
	$(MAKE) TOML_VERSION=$(DPK_LIB_KFP_VERSION) .defaults.update-toml

build:: .check-env .defaults.build-dist

publish:: .check-env  .defaults.publish-dist

.PHONY: venv
venv:
	rm -rf venv
	@# Help: Create the virtual environment using pyproject.toml
	$(MAKE) .defaults.ray-lib-src-venv
	. ${VENV_ACTIVATE}; pip install -e .	

test:: 	venv
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) api_params_test.py;
ifeq ($(DEPLOY_KUBEFLOW),1)
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) kuberay_api_test.py;
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) ray_remote_jobs_test.py;
endif