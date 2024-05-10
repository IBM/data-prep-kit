REPOROOT=${CURDIR}/../../../
# venv is common to all workflows
VENV_ACTIVATE=${REPOROOT}/kfp/transform_workflows/venv/bin/activate

# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../.make.transforms_workflows

PYTHON_WF := $(shell find ./ -name *_wf.py)
YAML_WF=$(patsubst %.py, %.yaml, ${PYTHON_WF})

venv:: .check_python_version ${VENV_ACTIVATE}

clean:: .transforms_workflows.clean

build:: 
	@for file in $(YAML_WF); do \
		$(MAKE) $$file;       \
	done
test::

upload::

.PHONY: reconcile-requirements
reconcile-requirements:
	@for file in $(PYTHON_WF); do \
		$(MAKE) .transforms_workflows.reconcile-requirement PIPELINE_FILE=$$file; \
	done
