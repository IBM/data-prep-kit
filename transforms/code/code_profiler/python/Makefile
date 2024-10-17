# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below.
include $(REPOROOT)/transforms/.make.transforms

# Include the common configuration for this transform
include ../transform.config

# values possible mach-arm64, x86_64
export RUNTIME_HOST_ARCH=x86_64

venv:: .transforms.python-venv

test::	.transforms.python-test

clean:: .transforms.clean

image:: .transforms.python-image

test-src:: .transforms.test-src

setup:: .transforms.setup

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(CODE_PROFILER_PYTHON_VERSION) TOML_VERSION=$(CODE_PROFILER_PYTHON_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

test-image:
	@echo "Skipping test-image step as per configuration."

# Ensure RUN_ARGS has a default value
RUN_ARGS ?= ""

# run-cli-sample: .transforms.run-cli-python-sample

run-local-sample: .transforms.run-local-sample

run-local-python-sample:
	$(MAKE) RUN_FILE=code_profiler_local_python.py \
	RUN_ARGS="--content 'Contents' --language 'Language'" \
	.transforms.run-local-python-sample