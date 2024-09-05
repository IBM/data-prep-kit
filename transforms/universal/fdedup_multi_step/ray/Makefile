# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=fdedup

BASE_IMAGE=${RAY_BASE_IMAGE}
venv::	.transforms.ray-venv

test::	.transforms.test-src test-image .transforms.clean

test-image-help::
	@# Help: Test an already-built image (use make image) to be sure the --help option works
	$(DOCKER) run -t --rm $(DOCKER_LOCAL_IMAGE) python fdedup_preprocessor_transform_ray.py --help
	$(DOCKER) run -t --rm $(DOCKER_LOCAL_IMAGE) python fdedup_bucket_processor_transform_ray.py --help
	$(DOCKER) run -t --rm $(DOCKER_LOCAL_IMAGE) python fdedup_filter_transform_ray.py --help


clean:: .transforms.clean

image:: .transforms.ray-image

test-src:: .transforms.test-src

setup:: .transforms.setup

test-image:: .transforms.ray-image test-image-help .defaults.test-image-pytest

build:: build-dist image

publish: publish-image

publish-image:: .transforms.publish-image-ray

setup:: .transforms.setup

# TRANSFORM_PYTHON_VERSION has no effect since requirements do not specify a python transform implementation
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=dummy TOML_VERSION=$(FDEDUP_RAY_VERSION) .transforms.set-versions 

build-dist:: .defaults.build-dist

publish-dist:: .defaults.publish-dist

run-cli-sample:
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_ray.py \
                RUN_ARGS="--run_locally True --data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../output'}\"  \
                --fdedup_id_column int_id_column"	\
                .transforms.run-src-file

run-local-sample: .transforms.run-local-ray-sample

run-s3-sample: .transforms.run-s3-ray-sample

minio-start:	.minio-start

kind-load-image:: .transforms.kind-load-image

docker-load-image: .defaults.docker-load-image

docker-save-image: .defaults.docker-save-image
