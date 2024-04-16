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

The transform projects all try to use a common set of conventions include code layout,
build, documentation and IDE recommendations.  For a transformed named `xyz`, it is
expected to have its project located under on of
`transforms/code/xyz`
`transforms/language/xyz`, OR
`transforms/universal/xyz`

### Project Organization
1. `src` directory contain python source for the transform with the following naming conventions/requirements.
    * `xyz_transform.py` generally contains the following:
        * `XYZTransform` class
        * `XYXTransformConfiguration` class
        * `XYZTransformRuntime` class, if needed.
        * main() to start the `TransformLauncher` with the above.
    * `xyz_local.py` - runs the transform on input to produce output w/o ray
    * `xyz_local_ray.py` - runs the transform in ray on data in `test-data/input` directory using the `TransformLauncher`
1. `test` directory contains pytest test sources
    * `test_xyz.py` - a standalone (non-ray launched) transform test.  This is best for initial debugging.
        * Inherits from an abstract test class so that to test one needs only to provide test data.
    * `test_xyz_launch.py` - runs ray via launcher.
        * Again, inherits from an abstract test class so that to test one needs only to provide test data.

   These are expected to be run from anywhere and so need to use
   `__file__` location to create absolute directory paths to the data in the `../test-data` directory.
   From the command line, `make test` sets up the virtual environment and PYTHONPATH to include `src`
   From the IDE, you **must** add the `src` directory to the project's Sources Root (see below).
   Do **not** add `sys.path.append(...)` in the test python code.
   All test data should be referenced as `../test-data`.
2. `test-data` contains any data file used by your tests.  Please don't put files over 5 MB here unless you really need to.
3. `requirements.txt` - used to create both the `venv` directory and docker image
4. A virtual environment (created in `venv` directory using `make venv`) is used for development and testing.
5. A generic `Dockerfile` is available that should be sufficient for most transforms.
6. `Makefile` is used for most common operations.
    * Should define `TRANSFORM_NAME=xyz` (see 1 above) - allows automation to reference correct files defined above.
    * Generally, defines the following targets for easy of operation.
        * help - shows all targets and help text
        * venv - builds the python virtual environment for CLI and IDE use
        * image - creates the docker image
        * test-src - sets up the virtual environment and runs test in the test directory.
        * test-image - runs the tests from within the image.
        * test - runs both test-src and test-image tests.

   The `Makefile` also defines a number of macros/variables that can be set, including the version of the docker image,
   python executable and more.

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

### Building the docker image
Generally to build a docker image, one uses the `make image` command, which uses
the `Dockerfile`, which in turn uses the `src` and `requirements.txt` to build the image.
Note that the `Makefile` defines the TRANSFORM_NAME and DOCKER_IMAGE_VERSION
and should be redefined if copying from another transform project.

### IDE Setup
When running in an IDE, such as PyCharm, the following are generally required:
* From the command line, build the venv using `make venv`.
* In the IDE
    * Set your project/run configuration to use the venv/bin/python as your runtime virtual environment.
        * In Pycharm this can be done through the PyCharm->Settings->Project...->Python Interpreter page
    * Mark the `src` as a _source root_ so that it is included in your PYTHONPATH when running .py files in the IDE
        * In Pycharm this can be done by selecting the `src` directory, and then selecting `Mark Directory as` -> `Sources Root`
 