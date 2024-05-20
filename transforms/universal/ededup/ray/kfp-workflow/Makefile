REPOROOT=${CURDIR}/../../../../..
WORKFLOW_VENV_ACTIVATE=${REPOROOT}/transforms/venv/bin/activate
include $(REPOROOT)/transforms/.make.transforms_workflows

SRC_DIR=${CURDIR}/../

YAML_FILE=ededup_wf.yaml

DOCKER_IMAGE_NAME=${TRANSFORM_NAME}
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${DOC_ID_VERSION}

workflow-venv: .check_python_version ${WORKFLOW_VENV_ACTIVATE}

.PHONY: workflow-build
workflow-build: workflow-venv
	$(MAKE) ${YAML_FILE} 

.PHONY: workflow-test
workflow-test: workflow-build
	$(MAKE) .transforms_workflows.test-pipeline TRANSFORM_SRC=${SRC_DIR} PIPELINE_FILE=${YAML_FILE}

.PHONY: workflow-upload
workflow-upload: workflow-build
	$(MAKE) .transforms_workflows.upload-pipeline PIPELINE_FILE=${YAML_FILE}

.PHONY: workflow-reconcile-requirements
workflow-reconcile-requirements:
	$(MAKE) .transforms_workflows.reconcile-requirement PIPELINE_FILE=ededup_wf.py
