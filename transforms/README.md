# Transforms

The transformation framework is designed to operate on rows of columnar data, generally contained
in [parquet](https://arrow.apache.org/docs/python/parquet.html) files
and read as [pyarrow tables](https://arrow.apache.org/docs/python/index.html).

Transforms are written to process the [table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
to, for example:

* Annotate the tables to add additional data such as document quality score, language, etc.
* Filter the table to remove or edit rows and/or columns, for example to remove rows from blocked domain.

While these transformation modules were originally built for pre-training, they are also useful for fine-tuning data preparation.

## Annotating Transforms
Annotating transforms examine 1 or more columns of data, typically a _content_ column containing a document
to be annotated.  The content is often spoken/text or programming language, generally to build
a large language model (LLM).  Examples of annotation might include:

* Language identification - an additional string column is added to identify the language of the document content.
* Document quality - an additional float column is added to associated a quality score with the document.
* Block listing - an addtional boolean column is added that indicates if the content source url
  (in one of the columns) is from a blocked domain.

## Filtering Transforms
Filtering transforms modify the rows and/or columns, usually based on associated column values.  
For example,

* Language selection - remove rows that do not match the desired language
* Document quality threshold - remove rows that do not meet a minimum document quality value.
* Block listing - remove rows that have been flagged as having been sourced from undesirable domains.

## Transform Organization
This directory hierarchy of transforms is organized as follows:

* `universal` - transforms applicable across code and language model data include
* `language` - spoken language model specific transforms
* `code` - programming language specific transforms.

Each of the `universal`, `language` and `code`  directories contains a directory for a specific transform.
Each transform is expected to be a standalone entity that generally runs at scale from within a docker image.
As such they each have their own virtual environments for development.

## Transform Project Conventions

The transform projects all try to use a common set of conventions including code layout,
build, documentation and IDE recommendations.  For a transformed named `xyz`, it is
expected to have its project located under one of

`transforms/code/xyz`   
`transforms/language/xyz`, OR   
`transforms/universal/xyz`.

### Makefile
The Makefile is the primary entry point for performing most functions
for the build and management of a transform.
This includes cleanup,
testing, creating the virtual environment, building
a docker image and more.
Use `make help` in any directory with a Makefile to see the available targets.
Each Makefile generally requires 
the following macro definitions:

* REPOROOT - specifies a relative path to the local directory
that is the root of the repository.
* TRANSFORM_NAME - specifies the simple name of the transform
that will be used in creating pypi artifacts and docker images.
* DOCKER_IMAGE_VERSION - sets the version of the docker image
and is usually set from one of the macros in `.make.versions` at the top
of the repository

These are used with the project conventions outlined below to 
build and manage the transform.

### Runtime Organization

Transforms support one or more _runtimes_ (e.,g python, Ray, Spark, KFP, etc).
Each runtime implementation is placed in a sub-directory under the transform's
primary directory, for example:

`transforms/universal/xyz/python`  
`transforms/universal/xyz/ray`  
`transforms/universal/xyz/spark`  
`transforms/universal/xyz/kfp`

A transform only need implement the python runtime, and the others generally build on this.

All runtime projects are structured as a _standard_ python project with the following:

* `src` - directory contains all implementation code
* `test` - directory contains test code
* `test-data` - directory containing data used in the tests
* `pyproject.toml` or `requirements.txt` (the latter is being phased out)
* `Makefile`- runs most operations, try `make help` to see a list of targets.
* `Dockerfile` to build the transform and runtime into a docker image 
* `output` - temporary directory capturing any test/local run output.  Ignored by .gitignore.


A virtual environment is created for the runtime project using `make venv`.

In general, all runtime-specific python files use an `_<runtime>.py>` suffix,
and docker images use a `-<runtime>` suffix in their names.  For example,

* `noop_transform_python.py`
* `test_noop_spark.py`
* `dpk-noop-transform-ray`

Finally, the command `make conventions` run from within a runtime
directory will examine the runtime project structure and make recommendations.

#### Python Runtime
The python runtime project contains the core transform implementation and
its configuration, along with the python-runtime classes to launch the transform.
The following organization and  naming conventions are strongly recommended
and in some cases required for the Makefile to do its work.

1. `src` directory contain python source for the transform with the following naming conventions/requirements.
    * `xyz_transform.py` generally contains the core transform implementation:
        * `XYZTransform` class implementing the transformation
        * `XYXTransformConfiguration` class that defines CLI configuration for the transform 
   * `xyz_transform_python.py` - runs the transform on input using the python runtime 
        * `XYZPythonTransformConfiguration` class
        * main() to start the `PythonTransformLauncher` with the above.
1. `test` directory contains pytest test sources
    * `test_xyz.py` - a standalone (non-ray launched) transform test.  This is best for initial debugging.
        * Inherits from an abstract test class so that to test one needs only to provide test data.
    * `test_xyz_python.py` - runs the transform via the Python launcher. 
        * Again, inherits from an abstract test class so that to test one needs only to provide test data.
         
   Tests are expected to be run from anywhere and so need to use
   `__file__` location to create absolute directory paths to the data in the `../test-data` directory.  
   From the command line, `make test` sets up the virtual environment and PYTHONPATH to include `src`
   From the IDE, you **must** add the `src` directory to the project's Sources Root (see below).
   Do **not** add `sys.path.append(...)` in the test python code.
   All test data should be referenced as `../test-data`.

#### Ray/Spark Runtimes
These projects are structured in a similar way and replace the python 
runtime source and test files with the following:

`src/xyz_transform_[ray|spark].py` 
    * `[Ray|Spark]TransformRuntimeConfiguration` - runtime configuration class
    * contains a main() that launches the runtime
`test/test_xyz_[ray|spark].py` - tests the transform running in the given runtime.

### Configuration and command line options
A transform generally accepts a dictionary of configuration to
control its operation.  For example, the size of a table, the location
of a model, etc. These are set either explicitly in dictionaries
(e.g. during testing) or from the command line when run from a Ray launcher.

When specified on the command line, transform `xyz` should use an `xyz` prefix with
`--xyz_` (dash dash) to define its command line options.
For example, `--xyz_some_cfg somevalue` sets
the value for the `xyz_some_cfg` configuration key value to `somevalue`.
To avoid potential collisions with options for the Ray launcher, Data Access Factory and others,
it is strongly encouraged to not use single dash options with a single
or small number of characters (e.g. -n).

## Release process
The transform versions are managed in a central file named [`.make.versions`](../.make.versions).
This file is where the versions are automatically propagated to the Makefile rules when building and pushing the transform images.
When a new transform version is created, the tag of the transform should be updated in this file.
If there is no entry for the transform in the file yet, create a new one and add a reference to it in the transform Makefile,
 following the format used for other transforms.
ore specifically, the entry should be of the following format: `<transform image name>_<RUNTIME>_VERSION=<version>`, 
for example: `FDEDUP_RAY_VERSION=0.2.77`

### Building the docker image
Generally to build a docker image, one uses the `make image` command, which uses
the `Dockerfile`, which in turn uses the `src` and `requirements.txt` to build the image.
Note that the `Makefile` defines the TRANSFORM_NAME and DOCKER_IMAGE_VERSION
and should be redefined if copying from another transform project.

To build individual transform image use `make -C <path to transform directory>`, for example: `make -C universal/fdedup image`.
To push all the images run `make push`, or `make -C <path to transform directory> push` for individual transform.

### IDE Setup
When running in an IDE, such as PyCharm or VS Code, the following are generally required:

* From the command line, build the venv using `make venv`.
* In the IDE
    * Set your project/run configuration to use the venv/bin/python as your runtime virtual environment.
        * In PyCharm, this can be done through the PyCharm->Settings->Project...->Python Interpreter page
        * In VS Code, click on the current Python Interpreter in the bottom right corner and make sure that the Interpreter path is venv/bin/python
    * Mark the `src` as a _source root_ so that it is included in your PYTHONPATH when running .py files in the IDE
        * In Pycharm this can be done by selecting the `src` directory, and then selecting `Mark Directory as` -> `Sources Root`
 
