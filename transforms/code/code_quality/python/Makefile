
# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../../..
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=code_quality
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${CODE_QUALITY_RAY_VERSION}

# Use default rule inherited from makefile.common
clean:: .transforms.clean

# Use default rule inherited from makefile.common
test:: .transforms.python-test

# Use default rule inherited from makefile.common
image:: .transforms.python-image

# Use default rule inherited from makefile.common
venv:: .transforms.python-venv

test-src:: .transforms.test-src

test-image:: .transforms.python-test-image

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TOML_VERSION=$(DOCKER_IMAGE_VERSION) .defaults.update-toml
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

setup:: .transforms.setup

run-cli-sample: .transforms.run-cli-python-sample

run-local-sample: #.transforms.run-local-sample
	@# Help: Not implemented yet
	@echo "No src/code_quality_local.py yet :("

run-local-sample: .transforms.run-local-python-sample


load-image:: .transforms.load-image
