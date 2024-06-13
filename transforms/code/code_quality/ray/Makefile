
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
test:: .transforms.ray-test

# Use default rule inherited from makefile.common
image:: .transforms.ray-image

# Use default rule inherited from makefile.common
venv:: .transforms.ray-venv

test-src:: .transforms.test-src

test-image:: .transforms.ray-test-image

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TOML_VERSION=$(DOCKER_IMAGE_VERSION) .defaults.update-toml
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

setup:: .transforms.setup

run-cli-ray-sample: .transforms.run-cli-ray-sample

run-local-sample: #.transforms.run-local-sample
	@# Help: Not implemented yet
	@echo "No src/code_quality_local.py yet :("

run-local-ray-sample: .transforms.run-local-ray-sample

run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

load-image:: .transforms.load-image
