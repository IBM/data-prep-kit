REPOROOT=../../
# Use make help, to see the available rules

include $(REPOROOT)/.make.defaults

include ../../.make.versions

DOCKER_IMAGE_VERSION=${INGEST_TO_PARQUET_VERSION}
DOCKER_IMAGE_NAME?=ingest2parquet
DOCKER_IMAGE=${DOCKER_REGISTRY_ENDPOINT}/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)


venv::	.defaults.lib-src-venv

build::	venv image

test::	test-src test-image

clean:: .defaults.clean

image:: .defaults.lib-src-image

test-src:: .defaults.test-src test-local

test-image:: image .defaults.test-image-pytest

test-local::
	@# Help: Run the *local*.py files in the src directory
	@source venv/bin/activate;		\
	export PYTHONPATH=src;			\
	files=$$(find src -name '*local*.py' );		\
	for i in $$files; do			\
	    echo Executing: python $$i;		\
	    python $$i;				\
	    if [ $$? -ne 0 ];	then		\
		exit 1;				\
	    fi;					\
	done



publish:: .transforms.publish
