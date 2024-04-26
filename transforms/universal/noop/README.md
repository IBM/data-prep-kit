# NOOP Transform 
Please see the set of
[transform project conventions](../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transforms serves as a template for transform writers as it does
not perform any transformations on the input (i.e., a no-operation transform).
As such it simply copies the input parquet files to the output directory.
It shows the basics of creating a simple 1:1 table transform.
It also implements a single configuration value to show how configuration
of the transform is implemented.

## Configuration and command line Options

The set of dictionary keys holding [NOOPTransform](src/noop_transform.py) 
configuration for values are as follows:

* _noop_sleep_sec_ - specifies the number of seconds to sleep during table transformation. 

## Running

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/noop_transform.py using command line args
* `run-local-sample` - runs src/noop_local.py
* `run-local-ray-sample` - runs src/noop_local_ray.py
* `run-s3-ray-sample` - runs src/noop_s3_ray.py

These targes will activate the virtual environment and set up any configuration needed.
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
