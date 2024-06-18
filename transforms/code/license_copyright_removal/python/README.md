# License and Copyright Removal
Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

This module is designed to detect and remove license and copyright information from code files. It leverages the [ScanCode Toolkit](https://pypi.org/project/scancode-toolkit/) to accurately identify and process licenses and copyrights in various programming languages.

After detecting license and copyright position new column names "updated_contents" has been created. Now lines which doesn't contain license or copyright copied to new column updated_column.

## Configuration and command line Options

The set of dictionary keys holding configuration for values are as follows:

* --license_copyright_removal_contents_column_name - specifies the column name which holds code content. By default the value is 'contents'.
* --license_copyright_removal_license - specifies the bool value for removing license or not. Default value is True.
* --license_copyright_removal_copyright - specifies the bool value for removing copyright or not. Default value is True. 

## Running
You can run the [license_copyright_removal_local.py](src/license_copyright_removal_local.py) (python-only implementation) or [license_copyright_removal_local_ray.py](ray/src/license_copyright_removal_local_ray.py) (ray-based  implementation) to transform the `test1.parquet` file in [test input data](test-data/input) to an `output` directory.  The directory will contain both the new annotated `test1.parquet` file and the `metadata.json` file.

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
the options provided by the [ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md)
and the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/license_copyright_removal_transform_python.py using command line args
* `run-local-python-sample` - runs src/license_copyright_removal_local_python.py
* `run-local-sample` - runs src/license_copyright_removal_local.py

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
