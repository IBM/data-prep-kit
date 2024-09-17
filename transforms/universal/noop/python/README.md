# NOOP Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transform serves as a template for transform writers as it does
not perform any transformations on the input (i.e., a no-operation transform).
As such, it simply copies the input parquet files to the output directory.
It shows the basics of creating a simple 1:1 table transform.
It also implements a single configuration value to show how configuration
of the transform is implemented.

## Configuration and command line Options

The set of dictionary keys holding [NOOPTransform](src/noop_transform.py) 
configuration for values are as follows:

* _noop_sleep_sec_ - specifies the number of seconds to sleep during the call the 
the `transform()` method of `NOOPTransformation`.  This may be useful for
simulating real transform timings and as a way to limit I/O on an S3 endpoint..
* _noop_pwd_ - specifies a dummy password not included in metadata. Provided
as an example of metadata that we want to not include in logging.

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --noop_sleep_sec NOOP_SLEEP_SEC
                        Sleep actor for a number of seconds while processing the data frame, before writing the file to COS
  --noop_pwd NOOP_PWD   A dummy password which should be filtered out of the metadata
```
These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/noop_transform.py using command line args
* `run-local-sample` - runs src/noop_local.py

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

