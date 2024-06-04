REPOROOT=${CURDIR}/../../../../../
WORKFLOW_VENV_ACTIVATE=${REPOROOT}/transforms/venv/bin/activate
include $(REPOROOT)/transforms/.make.workflows

SRC_DIR=${CURDIR}/../../ray/

YAML_FILE=ededup_wf.yaml

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
	$(MAKE) .transforms_workflows.reconcile-requirements PIPELINE_FILE=ededup_wf.py
