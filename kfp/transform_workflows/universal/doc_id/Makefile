REPOROOT=${CURDIR}/../../../../
# venv is common to all workflows
VENV_ACTIVATE=${REPOROOT}/kfp/transform_workflows/venv/bin/activate
PYTHON ?= python

# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms_workflows

SRC_DIR=${REPOROOT}/transforms/universal/doc_id/

DOCKER_IMAGE_NAME=doc_id
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${DOC_ID_VERSION}

venv:: .check_python_version ${VENV_ACTIVATE}

clean:: transforms_workflows.clean

build:: .check_python_version ${VENV_ACTIVATE}
	$(MAKE) doc_id_wf.yaml

test:: .check_python_version ${VENV_ACTIVATE}
	$(MAKE) .transforms_workflows.test-pipeline TRANSFORM_SRC=${SRC_DIR} PIPELINE_FILE=doc_id_wf.yaml
	
