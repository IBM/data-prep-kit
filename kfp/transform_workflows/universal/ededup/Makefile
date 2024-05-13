REPOROOT=${CURDIR}/../../../../
# venv is common to all workflows
VENV_ACTIVATE=${REPOROOT}/kfp/transform_workflows/venv/bin/activate

# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms_workflows

DOCKER_IMAGE_NAME=ededup

SRC_DIR=${REPOROOT}/transforms/universal/ededup

YAML_FILE=ededup_wf.yaml
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${EDEDUP_VERSION}

venv:: .check_python_version ${VENV_ACTIVATE}

clean:: .transforms_workflows.clean

build:: venv
	$(MAKE) ${YAML_FILE}

test:: build
	$(MAKE) .transforms_workflows.test-pipeline TRANSFORM_SRC=${SRC_DIR} PIPELINE_FILE=${YAML_FILE}

upload:: build
	$(MAKE) .transforms_workflows.upload-pipeline TRANSFORM_SRC=${SRC_DIR} PIPELINE_FILE=${YAML_FILE}

.PHONY: reconcile-requirements
reconcile-requirements:
	$(MAKE) .transforms_workflows.reconcile-requirement PIPELINE_FILE=ededup_wf.py
