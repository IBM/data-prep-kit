# Repository Structure and Use 

# Repository structure
* data_processing_lib - provides the core transform framework and library 
supporting data transformations in a Ray cluster
* transform
    * universal
        * ededup 
        * ...
    * code
        * code_quality 
        * ...
    * language
        * ...
* kfp - Kubeflow pipeline support
    * transform_workflows - implements pipelines over transform images.
       * universal 
           * ...
       * ...
* kind - kind


# Build and Makefiles
Makefiles are used for operations performed across all projects in the directory tree.
Using specific rules from the top of the repository tree will recurse their execution
into subdirectories  until subdirectories provide a Makefile that implements the action
and/or recurses further.  For example,
```shell
make test 
```
will apply the `make test` rule into all sub-directories supporting such recursion.
Try `make help` to see the set of available targets in a directory.  For example,
from the root of the repo...
```
Target               Description
------               -----------
build                Recursively build in all subdirs 
clean                Recursively clean in all subdirs 
setup                Recursively setup in all subdirs
test                 Recursively test in all subdirs 
```
or from a transform project directory
```
cd transforms/universal/noop
make help
Target               Description
------               -----------
build                Create the venv and build the transform image 
clean                Clean up the virtual environment.
conventions          Check transform project conventions and make recommendations, if needed.
image                Create the docker image quay.io/dataprep1/data-prep-lab/noop:0.7
publish              Publish the quay.io/dataprep1/data-prep-lab/noop:0.7 to quay.io container registry
setup                Do nothing, since nothing to setup by default. 
test                 Run both source and image level tests.
test-image           Test an quay.io/dataprep1/data-prep-lab/noop:0.7 use test source inside the image. 
test-locals          Run the *local*.py files in the src directory 
test-src             Run the transform's tests and any '*local' .py files
venv                 Install the source from the data processing library for python

Overridable macro values include the following:
DOCKER - the name of the docker executable to use. DOCKER=docker
DOCKER_FILE - the name of the docker file to use. DOCKER_FILE=Dockerfile
DOCKER_REGISTRY_ENDPOINT - the docker registry location to publish images. DOCKER_REGISTRY_ENDPOINT=quay.io/dataprep1/data-prep-lab
DOCKER_HOSTNAME - the name of the docker registry to use. DOCKER_HOSTNAME=quay.io
DOCKER_NAMESPACE - the name space to use in the registry. DOCKER_NAMESPACE=dataprep1
DOCKER_NAME - the name under the name space where images are publishes. DOCKER_NAME=data-prep-lab
DOCKER_REGISTRY_USER - the docker user to use. DOCKER_REGISTRY_USER=dataprep1
DOCKER_REGISTRY_KEY - the docker user to use. DOCKER_REGISTRY_KEY=secret
PYTHON - the python executable to use. PYTHON=python
DOCKER_IMAGE_NAME - the name of the docker image to produce. DOCKER_IMAGE_NAME=noop
TRANSFORM_SRC_FILE is the base name of the python source file containing the main() (e.g. noop_local_ray.py)

Macros that require definition in the including Makefile
REPOROOT defines the root directory of this repository (such as ../../..)
TRANSFORM_NAME defines the name of the transform and is used to define defaults for...
    DOCKER_IMAGE_NAME and TRANSFORM_SRC_FILE.  For, example 'noop'
DOCKER_IMAGE_VERSION - the version of the docker image to produce. DOCKER_IMAGE_VERSION=0.7
```

If you'd like to build each component separately, you can move into the sub-directories as desired.  
If planning to develop and/or use on Apple Mac please see these [considerations](mac.md).

## Data Prep Lab Library 
To build the wheel for the data processing library and publish it to a pypi... 
```shell
cd data-processing-lib 
make test build publish 
```

## Transforms
To create all transform images and publish them (by default to quay.io)
```shell
cd transforms
make venv test-src
make image test-image publish
```

