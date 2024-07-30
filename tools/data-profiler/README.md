# Data Profiler

This tool will generate an output with different metrics that can help a user quickly understand insights from the data (code and text) and see if the data is fit for their use. This can also be used to compare how things change between different data versions.

Data profiler can be usefulfor personas like Developers, Business Users, Data Scientists for use cases like,
* Understand the data before starting any data prep
* Understand data issues found during data prep
* Profile validation sets to understand where the model does well and where it does not do well
* Help customer build the right validation set based on their needs

Features:  
* Works on both code files and instruction pairs
* Data stats to help the user make recommendations during model tuning, eg * padding sizes
* Program Analysis based stats for data coverage
* Data prep-based stats on issues found in the data


The data profiler framework is designed to operate on rows of columnar data, generally contained
in [parquet](https://arrow.apache.org/docs/python/parquet.html) files
and read as [pyarrow tables](https://arrow.apache.org/docs/python/index.html).

Data profilers are written to process the [table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
and for dataset-wide statistics, syntactic information extraction, and semantic insights.

## Dataset-wide Statistics

Generates following stats about the datasets
* Instruction pairs
* Word counts
* Character counts
* Programming languages
* Human languages
* …

## Syntactic Information Extraction
Generates overall stats about the code snippets in the instruction pair of the Datasets
* Packages in the code data
* Modules in the code data
* ..

## Semantic Insights
* Concepts derived from the syntactic information such pakcage usage infomration
* ..

## Data Profiler Organization
This directory hierarchy of data profiler is organized as follows:

* `universal` - Profiler applicable across code and language model data include
* `language` - spoken language model specific profiler
* `code` - programming language specific profiler.

Each of the `universal`, `language` and `code`  directories contains a directory for a specific profiler.
Each profiler is expected to be a standalone entity that generally runs at scale from within a docker image [Support needs to added].
As such they each have their own virtual environments for development.

## Data Profiler Project Conventions

The profiler projects all try to use a common set of conventions including code layout,
build, documentation and IDE recommendations.  For a data profiler named `xyz`, it is
expected to have its project located under one of

`data-profiler/code/xyz`   
`data-profiler/language/xyz`, OR   
`data-profiler/universal/xyz`.

### Makefile
The Makefile is the primary entry point for performing most functions
for the build and management of a data profiler.
This includes cleanup,
testing, creating the virtual environment, building
a docker image and more.
Use `make help` in any directory with a Makefile to see the available targets.
Each Makefile generally requires 
the following macro definitions:

* REPOROOT - specifies a relative path to the local directory
that is the root of the repository.
* DATA_PROFILER_NAME - specifies the simple name of the profiler
that will be used in creating pypi artifacts and docker images.
* DOCKER_IMAGE_VERSION - sets the version of the docker image
and is usually set from one of the macros in `.make.versions` at the top
of the repository

These are used with the project conventions outlined below to 
build and manage the profilers.

### Runtime Organization

Profiler support one or more _runtimes_ (e.,g python, Ray, Spark, KFP, etc).
Each runtime implementation is placed in a sub-directory under the data profiler's
primary directory, for example:

`data-profiler/universal/xyz/python`  
`data-profiler/universal/xyz/ray`  
`data-profiler/universal/xyz/spark`        
`data-profiler/universal/xyz/kfp`

A profiler only need implement the python runtime, and the others generally build on this.

All runtime projects are structured as a _standard_ python project with the following:

* `src` - directory contains all implementation code
* `test` - directory contains test code
* `test-data` - directory containing data used in the tests
* `pyproject.toml` or `requirements.txt` (the latter is being phased out)
* `Makefile`- runs most operations, try `make help` to see a list of targets.
* `Dockerfile` to build the data profiler and runtime into a docker image [In progress]
* `output` - temporary directory capturing any test/local run output.  Ignored by .gitignore.


A virtual environment is created for the runtime project using `make venv`.

Currently, we support Python as a runtime. In general, all runtime-specific python files use an `_<runtime>.py>` suffix, and docker images use a `-<runtime>` suffix in their names.  For example,

* `dataset_statistics_python.py`
* `dataset_statistics_spark.py` [In progress]
* `dataset_statistics_ray.py` [In progress]

Finally, the command `make conventions` run from within a runtime
directory will examine the runtime project structure and make recommendations.

#### Python Runtime
The python runtime project contains the core profiler implementation and its configuration, along with the python-runtime classes to launch the profiler.

The following organization and  naming conventions are strongly recommended and in some cases required for the Makefile to do its work.

1. `src` directory contain python source for the profiler with the following naming conventions/requirements.
    * `xyz_profiler.py` generally contains the core profiler implementation:
        * `XYZProfiler` class implementing the profiling
        * `XYXProfilerConfiguration` class that defines CLI configuration for the profiler 
   * `xyz_profiler_python.py` - runs the profiler on input using the python runtime 
        * `XYZPythonProfilerConfiguration` class
        * main() to start the `PythonProfilerLauncher` with the above.
1. `test` directory contains pytest test sources
    * `test_xyz.py` - a standalone (non-ray launched) data profiler test.  This is best for initial debugging.
        * Inherits from an abstract test class so that to test one needs only to provide test data.
    * `test_xyz_python.py` - runs the data profiler via the Python launcher. 
        * Again, inherits from an abstract test class so that to test one needs only to provide test data.
         
   Tests are expected to be run from anywhere and so need to use
   `__file__` location to create absolute directory paths to the data in the `../test-data` directory.  
   From the command line, `make test` sets up the virtual environment and PYTHONPATH to include `src`
   From the IDE, you **must** add the `src` directory to the project's Sources Root (see below).
   Do **not** add `sys.path.append(...)` in the test python code.
   All test data should be referenced as `../test-data`.

#### Ray/Spark Runtimes  [In progress]
These projects are structured in a similar way and replace the python 
runtime source and test files with the following:

`src/xyz_profiler_[ray|spark].py` 
    * `[Ray|Spark]ProfilerRuntimeConfiguration` - runtime configuration class
    * contains a main() that launches the runtime
`test/test_xyz_[ray|spark].py` - tests the profiler running in the given runtime.

### Configuration and command line options

The configuration file to run the data profiler is present at the location `tools/data-profiler/input/data_profiler_params.json`. Our profilers support dynamic schema mapping, allowing you to assign column names from the input dataset schema. Before running the profiler for these data models, ensure that you set dynamic_schema_mapping to true in the settings.input_schema section.

If your dataset consists of instruction pairs, assign:

* the instruction column name to instruction_col
* the input code column name to input_code_col
* the output code column name to output_code_col

If your dataset contains a single column with code that you wish to profile, assign the column name to input_code_col. Additionally, if the code language is known and relevant, you can specify it using the code_language_col variable. The current profiler supports code profiling for Python and Java. If available, assign the code language to the code_language variable. If the dataset consists of instruction pairs then assign "SFT" to datatype and if it is a raw data then assign "EPT" to the datatype.

Currently, the profiler reads the dataset from a local location in your repository under directory `tools/data-profiler/input/qiskit.parquet`. We plan on adding the COS support as well in the upcoming releases.

When specified on the command line, profiler `xyz` should use an `xyz` prefix with
`--xyz_` (dash dash) to define its command line options. For example, `--xyz_some_cfg somevalue` sets the value for the `xyz_some_cfg` configuration key value to `somevalue`. To avoid potential collisions with options for the Ray launcher, Data Access Factory and others, it is strongly encouraged to not use single dash options with a single or small number of characters (e.g. -n).

## Release process
The profiler versions are managed in a central file named [`.make.versions`](../.make.versions).
This file is where the versions are automatically propagated to the Makefile rules when building and pushing the profiler images.
When a new profiler version is created, the tag of the profiler should be updated in this file.
If there is no entry for the profiler in the file yet, create a new one and add a reference to it in the profiler Makefile, following the format used for other profilers. More specifically, the entry should be of the following format: `<profiler image name>_<RUNTIME>_VERSION=<version>`, 
for example: `SEMANTIC_EXTRACTION_RAY_VERSION=0.2.77`

### Building the docker image [In progress]
Generally to build a docker image, one uses the `make image` command, which uses
the `Dockerfile`, which in turn uses the `src` and `requirements.txt` to build the image.
Note that the `Makefile` defines the PROFILER_NAME and DOCKER_IMAGE_VERSION
and should be redefined if copying from another profiler project.

To build individual profiler image use `make -C <path to profiler directory>`, for example: `make -C universal/fdedup image`.
To push all the images run `make push`, or `make -C <path to profiler directory> push` for individual profiler.

### IDE Setup
When running in an IDE, such as PyCharm or VS Code, the following are generally required:

* From the command line, build the venv using `make venv`.
* In the IDE
    * Set your project/run configuration to use the venv/bin/python as your runtime virtual environment.
        * In PyCharm, this can be done through the PyCharm->Settings->Project...->Python Interpreter page
        * In VS Code, click on the current Python Interpreter in the bottom right corner and make sure that the Interpreter path is venv/bin/python
    * Mark the `src` as a _source root_ so that it is included in your PYTHONPATH when running .py files in the IDE
        * In Pycharm this can be done by selecting the `src` directory, and then selecting `Mark Directory as` -> `Sources Root`
 
