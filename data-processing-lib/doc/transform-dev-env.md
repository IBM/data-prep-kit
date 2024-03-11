# Transform Development Environment

### Transform project conventions
The transform projects generally contain the following files and directories: 
1. `src` directory - contain all transform source.
   * `xyz_transform.py` - contains 
      * transform class
      * transform configuration class
      * Optional transform runtime class
      * main() that creates the launcher and launches it.
2. `test` directory - contains all tests for transform.
   *  `test_xyz.py` - contains AbstractTransformTest-based tests and extensions. 
3. `test-data` directory - contains any optional data required by tests. Please do
not put files large than half a MB here (to keep the repo small).
3. `requirements.txt` - defines all dependencies, including `fm-data-processing`.
4. `Dockerfile` - used to create the docker image containing the launchable transform.
5. `Makefile` with a standard set of rules used to define common operations. help is
a common target and produces the following (note the noop-specific macros)
```
$ make help
Target               Description
------               -----------
build                Create the virtual environment using requirements.txt
clean                Clean up the virtual environment.
image                Build the docker image using the Dockerfile and requirements.txt 
install-lib-src      Install the source from the data processing library for python 
test                 Use the already-built virtual environment to run pytest on the test directory. 
venv                 Create the virtual environment using requirements.txt

Overridable macro values include the following:
DOCKER - the name of the docker executable to use. DOCKER=podman
DOCKER_FILE - the name of the docker file to use. DOCKER_FILE=Dockerfile
DOCKER_REGISTRY - the name of the docker registry to use. DOCKER_REGISTRY=docker-na.artifactory.swg-devops.com/wcp-ai-foundation-team-docker-virtual
PPYTHON - the python executable to use. PYTHON=python

Macros that require definition in the including Makefile
DOCKER_IMAGE_NAME - the name of the docker image to produce. DOCKER_IMAGE_NAME=noop
DOCKER_IMAGE_VERSION - the version of the docker image to produce. DOCKER_IMAGE_VERSION=0.6

```  
As a starting template, you can copy the [noop](../../transforms/universal/noop) project,
but be sure to change the `DOCKER_IMAGE_NAME/VERSION` in the Makefile.

### Python environment and dependencies
It is strongly recommended to use a virtual environment (venv) to
install dependencies, including the `fm-data-processing` dependency.
Furthermore, each transform project should use its own virtual environment
so there is no contamination from non-essential dependencies and/or version
changes.
From within your transform project (e.g., noop, coalesce, etc.),
and after having defined your requirements in requirements.txt to
include
```
fm-data-processing=x.y.z
```
run the following
```
make venv
source venv/bin/activate
...
deactivate
```
If you wish to use the [latest source from 
data-processing-lib](../src) you can install the source over the 
`fm-data-processing` dependency using the `install_lib_source` makefile rule.
```
make install_lib_source
```

