# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

include $(REPOROOT)/transforms/.make.transforms


TRANSFORM_NAME=syntactic_concept_extractor

BASE_IMAGE=${RAY_BASE_IMAGE}
venv::	.transforms.ray-venv

clean:: .transforms.clean

test-src:: .transforms.test-src

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(SYNTACTIC_CONCEPT_EXTRACTOR_RAY_VERSION) TOML_VERSION=$(SYNTACTIC_CONCEPT_EXTRACTOR_RAY_VERSION) .transforms.set-versions 
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist


# Ensure RUN_ARGS has a default value
RUN_ARGS ?= ""

# run-cli-sample: .transforms.run-cli-ray-sample

run-local-ray-sample:
	@echo "Running local ray sample..."
	python src/syntactic_concept_extractor_local_ray.py \
	--data_local_config "{ 'input_folder' : './input', 'output_folder' : './output'}"
