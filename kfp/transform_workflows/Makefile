# Define the root of the local git clone for the common rules to be able
# know where they are running from.
export REPOROOT := ${CURDIR}/../..
include ${REPOROOT}/.make.versions

CURR_DIR := ${CURDIR}

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")

include makeenv
# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

include .make.transforms_workflows

export PYTHON=python
PYTEST=pytest
export VENV_ACTIVATE := ${CURR_DIR}/venv/bin/activate

PYTHON_WF := $(shell find ./ -name *_wf.py)
YAML_WF := $(patsubst %.py, %.yaml, ${PYTHON_WF})

clean::
	@# Help: Clean up the venv 
	rm -rf venv || true
	rm makeenv || true
	$(MAKE) RULE=$@ .recurse

setup::
	@# Help: Recursively $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

venv:: 
	@# Help: Recursively $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

build::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

test::
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse
