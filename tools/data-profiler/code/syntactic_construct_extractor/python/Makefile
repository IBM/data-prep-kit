# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

DATA_PROFILER_RUNTIME=python
DATA_PROFILER_NAME=syntactic_construct_extractor
DOCKER_IMAGE_VERSION=latest

# include $(REPOROOT)/transforms/.make.transforms
include $(REPOROOT)/tools/data-profiler/.make.profiler

.PHONY: venv test clean image build build-dist set-versions run-local-sample minio-stop

venv:: .data-profiler.python-venv

.data-profiler.python-venv:
	@echo "Setting up Python virtual environment..."
	python -m venv venv
	venv/bin/pip install -U pip setuptools wheel
	venv/bin/pip install .

test:: .data-profiler.python-test

.data-profiler.python-test: .data-profiler.python-venv
	@echo "Running Python tests..."
	venv/bin/pytest test/

clean:: .data-profiler.clean

image:: .data-profiler.python-image

build:: build-dist image

build-dist:: set-versions .defaults.build-dist 

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TOML_VERSION=1.1.0 .defaults.update-toml

run-local-sample:: .data-profiler.run-local-sample

minio-stop:
	@echo "Stopping MinIO..."
	# Add the actual command to stop MinIO
	docker stop minio || true
