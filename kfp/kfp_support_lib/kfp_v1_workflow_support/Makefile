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

clean::
	@# Help: Clean up the distribution build and the venv
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm -rf *.back || true


.check-env:: .check_python_version
	@echo "Checks passed"

.PHONY: set-versions
set-versions: 
	$(MAKE) TOML_VERSION=$(DPK_LIB_KFP_VERSION) .defaults.update-toml
	cat pyproject.toml | sed -e 's/"kfp\([=><][=><]\).*",/"kfp\1$(KFP_v1)",/' > tt.toml
	mv tt.toml pyproject.toml

build:: set-versions venv
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make build` in upper directories and KFPv2==1
	echo "Skipping build as KFPv2 is defined"
else
	@# Help: Build the distribution for publishing to a pypi
	$(MAKE) build-dist
endif

build-dist :: set-versions .defaults.build-dist

publish:: .check-env
	@# Help: Publish the wheel to testpypi
	if [ -d "dist"]; then rm -r dist; fi
	${PYTHON} -m pip install --upgrade build
	${PYTHON} -m twine check dist/*
	${PYTHON} -m twine upload --verbose --non-interactive dist/*

.PHONY: venv
venv:
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make venv` in upper directories and KFPv2==1
	echo "Skipping as KFPv2 is defined"
else
	@# Help: Create the virtual environment using pyproject.toml
	$(MAKE) .defaults.create-venv .defaults.install-ray-lib-src-venv
	@source venv/bin/activate; pip install -e ${REPOROOT}/kfp/kfp_support_lib/shared_workflow_support
	$(MAKE) .defaults.install-local-requirements-venv
endif

test:: venv 
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make test` in upper directories and KFPv2==1
	echo "Skipping test as KFPv2 is defined"
else
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
ifeq ($(DEPLOY_KUBEFLOW),1)
	. ${VENV_ACTIVATE}; export PYTHONPATH=../src; cd test;  $(PYTEST) pipeline_utils_test.py;
endif
endif
