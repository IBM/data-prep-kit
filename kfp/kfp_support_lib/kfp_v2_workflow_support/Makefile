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
	cat pyproject.toml | sed -e 's/"kfp\([=><][=><]\).*",/"kfp\1$(KFP_v2_SDK)",/' > tt.toml
	mv tt.toml pyproject.toml


build:: .check-env .defaults.build-dist

publish:: .check-env  .defaults.publish-dist

.PHONY: venv
venv:
	@# Help: Create the virtual environment using pyproject.toml
	$(MAKE) .defaults.create-venv .defaults.install-ray-lib-src-venv
	@source venv/bin/activate; pip install -e ${REPOROOT}/kfp/kfp_support_lib/shared_workflow_support
	$(MAKE) .defaults.install-local-requirements-venv

test:: 	venv
ifneq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make test` in upper directories and KFPv2 is not set
	echo "Skipping test as KFPv2 is not defined"
else
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
ifeq ($(DEPLOY_KUBEFLOW),1)
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) pipeline_utils_test.py;
endif
endif
