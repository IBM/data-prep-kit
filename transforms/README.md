# Transforms
This is the root director of all transforms.  It is organized as follows

* universal - holds transforms that may be useful across many domains
* language - holds transforms that are specific to language-based modeling 
* code - holds transforms that are specific to code-based modeling 

Each of these directories contains a number of directories, each directory implementing a specific transform.
Each transform is expected to be a standalone entity that generally runs at scale from within a docker image.
As such they each have their own virtual environments for development.

## Transform Project Conventions

The transform projects all try to use a common set of conventions include code layout, build, documentation and IDE recommendations.
 
### Project Organization
1. `src` directory contain python source for the transform.  `xyz_transform.py` 
generally contains the following:
    * `XYZTransform` class
    * `XYXTransformConfiguration` class
    * `XYZTransformRuntime` class, if needed.
    * main() to start the `TransformLauncher` with the above.
1. `test` directory contains pytest test sources 
    * `test_xyz.py` - a standalone (non-ray lauched) transform test.  This is best for initial debugging.
        * Inherits from an abstract test class so that to test one needs only to provide test data.
    * `test_xyz_launch.py` - runs ray via launcher. 
        * Again, inherits from an abstract test class so that to test one needs only to provide test data.
    * They are expected to be run from **within** the `test` directory.
        * From the command line, `make test` sets up the virtual environment and PYTHONPATH to include `src`
        * From the IDE, you **must** add the `src` directory to the project's Sources Root (see below).
        * Do **not** add `sys.path.append(...)` in the test python code.
        * All test data should be referenced as `../test-data`.
2. `test-data` contains any data file used by your tests.  Please don't put files over 5 MB here unless you really need to.
3. `requirements.txt` - used to create both the `venv` directory and docker image
4. A virtual environment (created in `venv` directory using `make venv`) is used for development and testing.
5. A generic `Dockerfile` is available that should be sufficient for most transforms.  
6. `Makefile` is used for most common operations
    * venv - builds the python virtual environment for CLI and IDE use
    * test - runs the test in the `test` directory.
    * image - creates the docker image
    * help - shows all targets and help text
The `Makefile` also defines a number of macros/variables that can be set, including the name and version of the docker image, 
python executable and more.

### Configuration and command line options
A transform generally accept a dictionary of configuration to
control its operation.  For example, the size of a table, the location
of a model, etc. These are set either explicitly in dictionaries
(e.g. during testing) or from the command line when run from a Ray launcher.

When specified on the command line, they are specified by prefixing with
`--` (dash dash).  For example, `--mytransform_some_cfg somevalue` sets 
the value for the `mytransform_some_cfg` configuration key value to `somevalue`. 

In general, a common prefix (i.e. `mytransform_`) is used to help distinguish these keys, primarily
for ease-of-use/readability command line use, logging, etc.  This is not required, but
strongly recommended.

### Building the docker image
Generally to build a docker image, one uses the `make image` command, which uses
the `Dockerfile`, which in turn uses the `src` and `requirements.txt` to build the image. 
Note that the `Makefile` defines the DOCKER_IMAGE_NAME and DOCKER_IMAGE_VERSION
and should be redefined if copying from another transform project.

### IDE Setup
When running in an IDE, such as PyCharm, the following are generally assumed:
* From the command line, build the venv using `make venv`.
* In the IDE
    * Set your project/run configuration to use the venv/bin/python as your runtime virtual environment.
        * In Pycharm this can be done through the PyCharm->Settings->Project...->Python Interpreter page
    * Mark the `src` as a _source root_ so that it is included in your PYTHONPATH when running .py files in the IDE
        * In Pycharm this can be done by selecting the `src` directory, and then selecting `Mark Directory as` -> `Sources Root`


