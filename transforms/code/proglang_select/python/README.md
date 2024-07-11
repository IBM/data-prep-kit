# Programming Language Select 

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

This is a transform which can be used while preprocessing code data. It allows the
user to specify the programming languages for which the data should be identifies as matching
a defined set of programming languages.
It adds a new annotation column which can specify boolean True/False based on whether the rows belong to the
specified programming languages. The rows which belongs to the programming languages which are
not matched are annotated as False.

It requires a text file specifying the allowed languages. It is specified by the
command line param `proglang_select_allowed_langs_file`. 
A sample file is included at `test-data/languages/allowed-code-languages.lst`.
The column specifying programming languages is to be specified by
commandline params `proglang_select_language_column`.

## Configuration and command line Options

The set of dictionary keys holding configuration for values are as follows:

* _proglang_select_allowed_langs_file_ - specifies the location of the list of supported languages
* _proglang_select_language_column_ - specifies the name of the column containing the language
* _proglang_select_output_column_ - specifies the name of the annotation column appended to the parquet. 
* _proglang_select_return_known_ - specifies whether to return supported or unsupported languages

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md)
and the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

```
  --proglang_select_allowed_langs_file PROGLANG_MATCH_ALLOWED_LANGS_FILE
                        Path to file containing the list of languages to be matched.
  --proglang_select_language_column PROGLANG_MATCH_LANGUAGE_COLUMN
                        The column name holding the name of the programming language assigned to the document
  --proglang_select_output_column PROGLANG_MATCH_OUTPUT_COLUMN
                        The column name to add and that contains the matching information
  --proglang_select_s3_cred PROGLANG_MATCH_S3_CRED
                        AST string of options for s3 credentials. Only required for S3 data access.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: optional s3 url
                        region: optional s3 region```
```


### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/proglang_select_transform.py using command line args
* `run-local-sample` - runs src/proglang_select_local.py
* `run-local-python-sample` - runs src/proglang_select_local_python.py

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-cli-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.

### Transforming local data 

Beginning with version 0.2.1, most/all python transform images are built with directories for mounting local data for processing.
Those directories are `/home/dpk/input` and `/home/dpk/output`.

After using `make image` to build the transform image, you can process the data 
in the `/home/me/input` directory and place it in the `/home/me/output` directory, for example,  using the 0.2.1 tagged image as follows:

```shell
docker run  --rm -v /home/me/input:/home/dpk/input -v /home/me/output:/home/dpk/output proglang_select-python:0.2.1 	\
	python proglang_select_transform_python.py --data_local_config "{ 'input_folder' : '/home/dpk/input', 'output_folder' : '/home/dpk/output'}"
```

You may also use the pre-built images on quay.io using `quay.io/dataprep1/data-prep-kit//proglang_select-python:0.2.1` as the image name.


### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
