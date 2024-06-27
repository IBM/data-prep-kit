# License and Copyright Removal
Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

This module is designed to detect and remove license and copyright information from code files. It leverages the [ScanCode Toolkit](https://pypi.org/project/scancode-toolkit/) to accurately identify and process licenses and copyrights in various programming languages.

After locating the position of license or copyright in the input code/sample, this module delete/remove those lines and returns the updated code as parquet file.

## Configuration and command line Options

The set of dictionary keys holding configuration for values are as follows:

* --header_cleanser_contents_column_name - specifies the column name which holds code content. By default the value is 'contents'.
* --header_cleanser_license - specifies the bool value for removing license or not. Write true or false.  Default value is True.
* --header_cleanser_copyright - specifies the bool value for removing copyright or not. Write true or false. Default value is True. 

## Running
You can run the [header_cleanser_local.py](src/header_cleanser_local.py) (python-only implementation) or [header_cleanser_local_ray.py](ray/src/header_cleanser_local_ray.py) (ray-based  implementation) to transform the `test1.parquet` file in [test input data](test-data/input) to an `output` directory.  The directory will contain both the new annotated `test1.parquet` file and the `metadata.json` file.

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/header_cleanser_transform_python.py using command line args
* `run-local-python-sample` - runs src/header_cleanser_local_python.py
* `run-local-sample` - runs src/header_cleanser_local.py

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
