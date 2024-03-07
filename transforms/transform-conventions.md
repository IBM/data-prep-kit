# Transform Conventions

The transform projects leverage the recursive `make` targets defined at the top of the repo (e.g. build, clean, test, etc).
Transform projects are standalone entities.  Each transform is expected to be built into a separate docker image.  As such
they each have their own virtual environments.
 
## Project Organization
1. `src` directory contain python source for the transform.  `xyz_transform.py` 
generally contains the following:
    * `XYZTransform` class
    * `XYXTransformConfiguration` class
    * `XYZTransformRuntime` class, if needed.
    * main() to start the `TransformLauncher` with the above.
1. `test` directory contains test sources - usually a standalone test and ray launcher test.
1. `requirements.txt` - used to create both the `venv` directory and docker image
1. A virtual environment (created in `venv` directory) is used for development and testing.
1. A generic `Dockerfile` is available that should be sufficient for most transforms.  
1. Makefile is used for most common operations
    * venv - builds the python virtual environment for CLI and IDE use
    * test - runs the test in the `test` directory.
    * build - creates the docker image
The `Makefile` also defines a number of macros/variables that can be set, including the name and version of the docker image, 
python executable and more.

## Configuration and command line options
Transforms generally accept a dictionary of configuration to
control its operation.  For example, the size of a table, the location
of a model, etc. These are set either explicitly in dictionaries
(e.g. during testing) or from the command line when run from a Ray launcher.

When specified on the command line, they are specified by prefixing with
`--` (dash dash).  For example, `--mytransform_some_cfg somevalue` sets 
the value for the `mytransform_some_cfg` configuration key value to `somevalue`. 

In general, a common prefix (i.e. `mytransform_`) is used to help distinguish these keys, primarily
for ease-of-use/readability command line use, logging, etc.  This is not required, but
strongly recommended.

## Building the docker image
Generally to build a docker image, one uses the `make build` command, which uses
the `Dockerfile`, which in turn uses the `src` and `requirements.txt` to build the image. 
Note that the `Makefile` defines the DOCKER_IMAGE_NAME and DOCKER_IMAGE_VERSION.

## IDE Setup
When running in an IDE, such as PyCharm, the following are generally assumed:
* Build the venv using `make venv` and add this as a virtual environment to the IDE's project.
* Mark the `src` as a _source root_ so that it is included in your PYTHONPATH when running .py files in the IDE
  * In Pycharm this can be done by selecting the `src` directory, and then
  selecting `Mark Directory as` -> `Sources Root`


