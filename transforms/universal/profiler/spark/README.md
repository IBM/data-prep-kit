# Profiler Transform 

Per the set of 
[transform project conventions](../../../README.md#transform-project-conventions)
the following runtimes are available:

* [ray](../ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp_ray](../kfp_ray/README.md) - enables running the ray docker image for
the transformer in a kubernetes cluster using a generated `yaml` file.

## Summary

This project wraps the [profiler transform](../python) with a Spark runtime.

## Transform runtime

[Transform runtime](src/profiler_transform_ray.py) is responsible for creation cache actors and sending their
handles to the transforms themselves
Additionally it writes created word counts to the data storage (as .csv files) and enhances statistics information with the information about cache size and utilization

## Configuration and command line Options

Spark version uses the same configuration parameters as the [Python one](../python/README.md)

### Launched Command Line Options
When running the transform with the Spark launcher (i.e. TransformLauncher),
in addition to command line arguments provided by the [launcher](../../../../data-processing-lib/doc/spark-launcher-options.md).
the same arguments are available as for the [python one](../python/README.md)

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/ededup_transform_ray.py using command line args
* `run-local-sample` - runs src/ededup_local_ray.py
* `run-s3-sample` - runs src/ededup_s3_ray.py
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

