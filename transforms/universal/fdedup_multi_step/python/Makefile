# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

TRANSFORM_NAME=fdedup

include $(REPOROOT)/transforms/.make.transforms

venv::	.transforms.python-venv

test::	.transforms.test-src test-image .transforms.clean

test-image-help::
	@# Help: Test an already-built image (use make image) to be sure the --help option works
	$(DOCKER) run -t --rm $(DOCKER_LOCAL_IMAGE) python fdedup_preprocessor_transform.py --help
	$(DOCKER) run -t --rm $(DOCKER_LOCAL_IMAGE) python fdedup_bucket_processor_transform.py --help
	$(DOCKER) run -t --rm $(DOCKER_LOCAL_IMAGE) python fdedup_filter_transform.py --help

clean:: .transforms.clean

image:: .transforms.python-image

test-src:: .transforms.test-src

setup:: .transforms.setup

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(EDEDUP_PYTHON_VERSION) TOML_VERSION=$(EDEDUP_PYTHON_VERSION) .transforms.set-versions
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

test-image:: .transforms.python-image test-image-help .defaults.test-image-pytest

run-cli-sample: .transforms.run-cli-python-sample

run-local-sample: .transforms.run-local-sample

run-local-python-sample: .transforms.run-local-python-sample

#run-s3-ray-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image
