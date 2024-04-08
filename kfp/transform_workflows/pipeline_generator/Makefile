ROOT_DIR := ${CURDIR}

# Use make help, to see the available rules
include ../../../.make.defaults

PIPELINE_DEFINITION ?= ${ROOT_DIR}/example/pipeline_definitions.yaml
PIPELINE_DIST_DIR ?= ${ROOT_DIR}/../universal/

PYTHON=python
VENV_ACTIVATE=venv/bin/activate

venv::  
	@# Help: Create the virtual environment
	rm -rf venv
	$(PYTHON) -m venv venv
	. ${VENV_ACTIVATE};     \
	pip install pyyaml \
	pip install pre-commit

generate_pipeline::
	@# Help: Generate a new pipeline using the pipeline generator
	$(call check_defined, PIPELINE_DIST_DIR)
	$(call check_defined, PIPELINE_DEFINITION)
	. ${VENV_ACTIVATE} && ${PYTHON} pipeline_generator.py -c ${PIPELINE_DEFINITION} -od ${PIPELINE_DIST_DIR}
