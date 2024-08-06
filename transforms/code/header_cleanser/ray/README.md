# Header cleanser

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary
This module is designed to detect and remove license and copyright information from code files. It leverages the [ScanCode Toolkit](https://pypi.org/project/scancode-toolkit/) to accurately identify and process licenses and copyrights in various programming languages.

After locating the position of license or copyright in the input code/sample, this module delete/remove those lines and returns the updated code as parquet file.

## Configuration and command line Options

This project wraps the [header cleanser transform](../python) with a Ray runtime.

## Running

### Launched Command Line Options 
In addition to those available to the transform as defined in [here](../python/README.md), the set of [ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md) are available.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-ray-sample` - runs src/header_cleanser_transform.py using command line args
* `run-local-ray-sample` - runs src/header_cleanser_local_ray.py
* `run-s3-ray-sample` - runs src/header_cleanser_s3_ray.py
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

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.