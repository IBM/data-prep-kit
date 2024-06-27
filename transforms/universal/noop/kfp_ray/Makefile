REPOROOT=${CURDIR}/../../../../
WORKFLOW_VENV_ACTIVATE=${REPOROOT}/transforms/venv/bin/activate
include $(REPOROOT)/transforms/.make.workflows

SRC_DIR=${CURDIR}/../ray/

PYTHON_WF := $(shell find ./ -name '*_wf.py')
YAML_WF := $(patsubst %.py, %.yaml, ${PYTHON_WF})

workflow-venv: .check_python_version ${WORKFLOW_VENV_ACTIVATE}

.PHONY: clean
clean:
	@# Help: Clean up the virtual environment.
	rm -rf ${REPOROOT}/transforms/venv 

venv::

build::

setup::

test::

test-src::

test-image::

publish::

image::

load-image::

.PHONY: workflow-build
workflow-build: workflow-venv
	$(MAKE) $(YAML_WF)

.PHONY: workflow-test
workflow-test: workflow-build
	$(MAKE) .workflows.test-pipeline TRANSFORM_SRC=${SRC_DIR} PIPELINE_FILE=noop_wf.yaml

.PHONY: workflow-upload
workflow-upload: workflow-build
	@for file in $(YAML_WF); do \
		$(MAKE) .workflows.upload-pipeline PIPELINE_FILE=$$file; \
	done
