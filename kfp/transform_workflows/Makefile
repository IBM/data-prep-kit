# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../..
include ${REPOROOT}/.make.versions

CURR_DIR := ${CURDIR}

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")


include makeenv
# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

include .make.transforms_workflows

PYTHON=python3
PYTEST=pytest
VENV_ACTIVATE=venv/bin/activate

PYTHON_WF := $(shell find ./ -name *_wf.py)
YAML_WF=$(patsubst %.py, %.yaml, ${PYTHON_WF})

clean::
	@# Help: Clean up the distribution build and the venv 
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm makeenv || true
	find . -iname "*_wf.yaml" |  xargs rm -f || true

.check-env:: .check_python_version
	@echo "Checks passed"

.compile-pipeline::
	. ${VENV_ACTIVATE} && cd ${DIR} && ${PYTHON} ${WF_NAME}.py

%.yaml: %.py
	sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_TAGNAME}\"/" $<
	make -C $(shell (dirname $<)) .reconcile-requirement PIPELINE_FILE=${CURR_DIR}/$<
	make .compile-pipeline WF_NAME=$(shell (basename $< .py)) DIR=$(shell (dirname $<))

build::
	find . -iname "*_wf.yaml" |  xargs rm -f || true
	@# Help: Compile the pipelines
	${PYTHON} -m pip install --upgrade build
	make ${YAML_WF}

publish:: .check-env
	@# Help: Publish the wheel to and pypi
	@${PYTHON} -m twine upload --verbose --non-interactive --skip-existing --repository testpypi dist/*

venv::	.check-env
	@# Help: Create the virtual environment
	sed -i.back "s/data-prep-lab-kfp==[0-9].*/data-prep-lab-kfp==${DPL_LIB_KFP_VERSION}/" ../kfp_ray_components/requirements.txt
	rm -rf venv	
	$(PYTHON) -m venv venv
	. ${VENV_ACTIVATE}; 	\
	pip install kfp==${KFP} --extra-index-url https://pypi.org/simple \
	pip install -r ../kfp_ray_components/requirements.txt

test::
