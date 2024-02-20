include ../makefile.include

VERSION=0.2.0
TAG := "v${VERSION}"
LIBNAME=fm_data_processing
PYTHON=python3
#ARTIFACTORY_DESTINATION=https://na.artifactory.swg-devops.com/artifactory/api/pypi/res-edge-ai-team-sdk-pypi-local
#ARTIFACTORY_DESTINATION=https://na.artifactory.swg-devops.com/artifactory/api/pypi/wcp-ai-foundation-team-fms-pypi-local
ARTIFACTORY_DESTINATION=https://na.artifactory.swg-devops.com/artifactory/api/pypi/res-data-engineering-team-pypi-local

clean:
	rm -r dist
	rm -rf src/*egg-info

check-env:
	@if [ -z "$(VERSION)" ]; then echo "You must provide VERSION"; false; fi
	@if [ -z "$(ARTIFACTORY_USER)" ] || [ -z "$(ARTIFACTORY_API_KEY)" ]; then echo "You must set ARTIFACTORY_USER and ARTIFACTORY_API_KEY env vars"; false ; fi
	@echo "Checks passed"

update-toml: check-env
	sed -e 's/^version[ ]*=.*/version = "'${VERSION}'"/' pyproject.toml > tt.toml
	mv tt.toml pyproject.toml

build: update-toml 
	${PYTHON} -m pip install --upgrade build
	${PYTHON} -m build

publish: check-env
	@twine upload --verbose --non-interactive --skip-existing \
		--repository-url ${ARTIFACTORY_DESTINATION}	\
		-u ${ARTIFACTORY_USER} \
		-p ${ARTIFACTORY_API_KEY} \
		dist/${LIBNAME}-${VERSION}-py3-none-any.whl
	#@echo "create a git tag to reference published version"
	#@git tag ${TAG}
	#@git push origin ${TAG}

venv:	pyproject.toml
	rm -rf venv	
	python -m venv venv
	source venv/bin/activate; 	\
	pip install --upgrade pip;	\
	pip install -e .;		\
	pip install pytest
