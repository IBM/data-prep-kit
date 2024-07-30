# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

DATA_PROFILER_RUNTIME=python
DATA_PROFILER_NAME=dataset_stats
DOCKER_IMAGE_VERSION=latest


# include $(REPOROOT)/transforms/.make.transforms
include $(REPOROOT)/tools/data-profiler/.make.profiler

venv::	.data-profiler.python-venv

test::	.data-profiler.python-test

clean:: .data-profiler.clean

image:: .data-profiler.python-image

build:: build-dist image

build-dist:: set-versions .defaults.build-dist 

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TOML_VERSION=1.1.0 .defaults.update-toml

run-local-sample: .data-profiler.run-local-sample