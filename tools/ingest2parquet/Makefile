REPOROOT=../../
# Use make help, to see the available rules
include ./.make.tools
include $(REPOROOT)/.make.defaults


DOCKER_IMAGE_VERSION=${INGEST_TO_PARQUET_VERSION}
DOCKER_IMAGE_NAME?=ingest2parquet
DOCKER_IMAGE=${DOCKER_REGISTRY_ENDPOINT}/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)


venv::	.defaults.lib-src-venv

build::	venv image

test::	venv test-src test-image

clean:: .defaults.clean

image:: .defaults.lib-src-image

test-src:: .defaults.test-src test-local

test-image: image .defaults.test-image-pytest

minio-load:
	$(MAKE) MINIO_SRC=test-data/input MINIO_DEST=ingest2parquet/input .defaults.minio.load-test-data

minio-start: .defaults.minio.start

minio-stop:	.defaults.minio.stop 

test-local:: .defaults.test-locals

publish:: image .defaults.publish

run-local-sample:
	$(MAKE) RUN_FILE=ingest2parquet_local.py  .tools.run-src-file
	
run-s3-sample:
	$(MAKE) minio-start minio-load RUN_FILE=ingest2parquet_s3.py .tools.run-src-file
	@# Help: Run src/ingest2parquet_s3.py file (if it exists).  Assumes minio has beens started.
	@echo ""
	@echo "You may want to stop the minio server now (see make help)"
	@echo ""