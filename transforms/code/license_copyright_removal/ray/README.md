# License copyright removal 

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary
This module is designed to detect and remove license and copyright information from code files. It leverages the [ScanCode Toolkit](https://pypi.org/project/scancode-toolkit/) to accurately identify and process licenses and copyrights in various programming languages.

After detecting license and copyright position new column names updated_contents has been created. Now lines which doesn't contain license or copyright copied to new column updated_column.

## Configuration and command line Options

The set of dictionary keys holding configuration for values are as follows:

* --license_copyright_removal_contents_column_name - specifies the column name which holds code content. By default the value is 'contents'.
* --license_copyright_removal_license - specifies the bool value for removing license or not. Default value is True.
* --license_copyright_removal_copyright - specifies the bool value for removing copyright or not. Default value is True. 

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher), In addition to those available to the transform as defined in [here](https://github.com/IBM/data-prep-kit/blob/dev/transforms/universal/filter/python/README.md), the set of [ray launcher](https://github.com/IBM/data-prep-kit/blob/dev/data-processing-lib/doc/ray-launcher-options.md) are available.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-ray-sample` - runs src/license_copyright_removal_transform.py using command line args
* `run-local-ray-sample` - runs src/license_copyright_removal_local_ray.py
* `run-s3-ray-sample` - runs src/license_copyright_removal_s3_ray.py
    * Requires prior invocation of `make minio-start` to load data into local minio for S3 access.

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-cli-ray-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.
