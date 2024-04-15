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

## Testing transforms with S3

For testing transforms with S3 we are using [Minio](https://min.io/), which can be installed on
[Linux](https://min.io/docs/minio/linux/index.html), [macOS](https://min.io/docs/minio/macos/index.html) and
[Windows](https://min.io/docs/minio/windows/index.html). Here we are assuming Mac usage, refer to documentation
above for other platforms.

### Installing Minio

The simplest way to install Minio on Mac is using Homebrew. Use the following command:

```shell
brew install minio/stable/minio
```

In addition to the Minio server install the latest stable MinIO cli using

```shell
brew install minio/stable/mc
```
Now you can start Minio server using the following command:

```shell
minio server start
```

When it starts you can connect to the server UI using the following address: `https://localhost:9000`
The default user name/password is `minioadmin|minioadmin`

### Populating Minio with testing data

Populating Minio server with test data can be done using `mc`. First configure mc to work with the local
Minio server:

```shell
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
```

This set an alias `local` to 'mc' connected to the local Minio server instance. Now we can use our
mc instance to populate server using a set of
[commands](https://min.io/docs/minio/linux/reference/minio-mc.html) provided by `mc`.

First test the connection to the newly added MinIO deployment using the `mc admin info` command:

```shell
mc admin info local
```

To copy the data to Minio, you first need to create a bucket:

```shell
mc mb loca/test
```

Once the bucket is created, you can copy files, using:

```shell
mc cp --recursive code/code_quality/test-data/input/ local/test/code_quality/input
mc cp --recursive code/language_annotator/test-data/input/ local/test/language_annotator/input
mc cp --recursive code/language_annotator/test-data/languages/ local/test/lang_annotator/languages
mc cp --recursive code/malware/test-data/input/ local/test/malware/input

mc cp --recursive language/doc_quality/test-data/input/ local/test/doc_quality/input
mc cp --recursive language/language_id/test-data/input/ local/test/language_id/input

mc cp --recursive universal/blocklist/test-data/input/ local/test/blocklist/input
mc cp --recursive universal/blocklist/test-data/domains/ local/test/blocklist/domains
mc cp --recursive universal/doc_id/test-data/input/ local/test/doc_id/input
mc cp --recursive universal/ededup/test-data/input/ local/test/ededup/input
mc cp --recursive universal/fdedup/test-data/input/ local/test/fdedup/input
mc cp --recursive universal/filter/test-data/input/ local/test/filter/input
mc cp --recursive universal/noop/test-data/input/ local/test/noop/input
mc cp --recursive universal/resize/test-data/input/ local/test/resize/input
mc cp --recursive universal/tokenization/test-data/ds01/input/ local/test/tokenization/ds01/input
mc cp --recursive universal/tokenization/test-data/ds02/input/ local/test/tokenization/ds02/input
```

*Note*, that once the data is copied, Minio is storing it on the local file system, so you do not need to
copy it again after cluster restart

The last thing is to add Minio access and secret keys for accessing it. The following command:

```shell
mc admin user svcacct add --access-key "localminioaccesskey" --secret-key "localminiosecretkey" local minioadmin
```

creates both access and secret key for usage by the applications
    