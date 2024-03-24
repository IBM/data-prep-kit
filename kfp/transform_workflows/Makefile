# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../..

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")
IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/kfp/requirements.env | sed 's/\$\([A-Z_]*\)\-\([a-z]*\)/{\1\}\-\2/g' | sed 's/=/:=/' | sed 's/^/export /' >> makeenv")
include makeenv
# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

PYTHON=python3
PYTEST=pytest
VENV_ACTIVATE=venv/bin/activate

PYTHON_WF := $(shell find ./ -name *_wf.py)
YAML_WF=$(patsubst %.py, %.yaml, ${PYTHON_WF})

clean::
	@# Help: Clean up the distribution build and the venv 
	rm -r dist venv
	rm -rf src/*egg-info
	rm makeenv

.check-env::
	@if [ -z "$(FM_DATA_PROCESSING_KFP)" ]; then echo "You must provide FM_DATA_PROCESSING_KFP"; false; fi
	@echo "Checks passed"

.compile-pipeline::
	. ${VENV_ACTIVATE} && cd ${DIR} && ${PYTHON} ${WF_NAME}.py

%.yaml: %.py
	make .compile-pipeline WF_NAME=$(shell (basename $< .py)) DIR=$(shell (dirname $<))

.reconcile-requirement::
	@for file in $(PYTHON_WF); do \
		sed -i.back "s/kfp-data-processing:[0-9].*/kfp-data-processing:${KFP_DOCKER_TAGNAME}\"/" $$file; \
	done

build:: venv
	@# Help: Compile the pipelines
	${PYTHON} -m pip install --upgrade build
	make .reconcile-requirement
	make ${YAML_WF}

publish:: .check-env
	@# Help: Publish the wheel to and pypi
	@${PYTHON} -m twine upload --verbose --non-interactive --skip-existing --repository testpypi dist/*

venv::	.check-env
	@# Help: Create the virtual environment
	$(call check_defined, ARTIFACTORY_USER)
	$(call check_defined, ARTIFACTORY_API_KEY)
	sed -i.back "s/fm-data-processing==[0-9].*/fm-data-processing==${FM_DATA_PROCESSING}/" ../kfp_ray_components/requirements.txt
	rm -rf venv	
	$(PYTHON) -m venv venv
	. ${VENV_ACTIVATE}; 	\
	pip install kfp==${KFP} --extra-index-url https://pypi.org/simple \
	pip install -r ../kfp_ray_components/requirements.txt \
	pip install -i https://test.pypi.org/simple/ fm-data-processing-kfp==${FM_DATA_PROCESSING_KFP}

test::
