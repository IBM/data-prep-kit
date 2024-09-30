# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

TRANSFORM_NAME=syntactic_concept_extractor

include $(REPOROOT)/transforms/.make.transforms

venv::	.transforms.python-venv

clean:: .transforms.clean

test-src:: .transforms.test-src

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(SYNTACTIC_CONCEPT_EXTRACTOR_PYTHON_VERSION) TOML_VERSION=$(SYNTACTIC_CONCEPT_EXTRACTOR_PYTHON_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

# Ensure RUN_ARGS has a default value
RUN_ARGS ?= ""

# run-cli-sample: .transforms.run-cli-python-sample

run-local-sample: .transforms.run-local-sample

run-local-python-sample:
	$(MAKE) RUN_FILE=syntactic_concept_extractor_local_python.py \
	RUN_ARGS="--content 'Contents' --language 'Language'" \
	.transforms.run-local-python-sample