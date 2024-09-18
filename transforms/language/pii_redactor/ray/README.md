# PII Redactor Ray Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This project wraps the [pii redactor transform](../python) with a Ray runtime.

## Configuration and command line Options

PII redactor configuration and command line options are the same as for the [base python](../python) transform and in additional it also supports other options refer [pii redactor transform](../python/README.md) . 

## Running

### Launched Command Line Options 
In addition to those available to the transform as defined in [here](../python/README.md),
the set of 
[ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md) are available.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/pii_redactor_transform.py using command line args
* `run-local-sample` - runs src/pii_redactor_local_ray.py
* `run-s3-sample` - runs src/pii_redactor_s3_ray.py
    * Requires prior installation of minio, depending on your platform (e.g., from [here](https://min.io/docs/minio/macos/index.html)
     and [here](https://min.io/docs/minio/linux/index.html) 
     and invocation of `make minio-start` to load data into local minio for S3 access.

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

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
