REPOROOT=${CURDIR}/../../../..
WORKFLOW_VENV_ACTIVATE=${REPOROOT}/transforms/venv/bin/activate
include $(REPOROOT)/transforms/.make.workflows

PYTHON_WF := $(shell find ./ -name "*_wf.py")
YAML_WF=$(patsubst %.py, %.yaml, ${PYTHON_WF})

workflow-venv:: .check_python_version ${WORKFLOW_VENV_ACTIVATE}

worflow-clean:: .workflows.clean

.PHONY: workflow-build
workflow-build: workflow-venv
	@for file in $(YAML_WF); do \
		$(MAKE) $$file;       \
	done

workflow-test::

.PHONY: workflow-upload
workflow-upload: workflow-build
	@for file in $(YAML_WF); do \
                $(MAKE) .workflows.upload-pipeline PIPELINE_FILE=$$file; \
        done

.PHONY: clean
clean:
	rm -rf *.back || true
	rm -rf $(REPOROOT)/transforms/venv
