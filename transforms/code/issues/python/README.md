
# Issues Transform for Python Runtime
Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

Filter gitHub issues based on comments, description, and authors. This originally comes from [bigcode issues filter](https://raw.githubusercontent.com/bigcode-project/bigcode-dataset/main/preprocessing/filtering_issues.py).

## Configuration and command line Options

The set of dictionary keys holding [IssuesTransform](src/issues_transform_python.py) 
configuration for values are as follows:

* _min_chars_ - The minimum characters that text of issue with only one user is required to contain. 
* _max_chars_ - The maximum characters that text of issue with only one user is allowed to contain.
* _max_events_ - The maximum events that issue with only one user is allowed to contain.
* _max_lines_ - The maximum lines that text of issue can hold. The longer text will be truncated.
* _events_column_name_ - The column name to get the list of events.

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

```
  --issues_min_chars ISSUES_MIN_CHARS
                        The minimum characters that text of issue with only one user is required to contain
  --issues_max_chars ISSUES_MAX_CHARS
                        The maximum characters that text of issue with only one user is allowed to contain
  --issues_max_events ISSUES_MAX_EVENTS
                        The maximum events that issue with only one user is allowed to contain
  --issues_max_lines ISSUES_MAX_LINES
                        The maximum lines that text of issue can hold. The longer text will be truncated.
  --issues_events_column_name ISSUES_EVENTS_COLUMN_NAME
                        The column name to get the list of events.

```

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/filter_transform_ray.py using command line args
* `run-local-python-only-sample` - runs src/filter_local.py
* `run-local-sample` - runs src/filter_local_ray.py
* `run-s3-sample` - runs src/filter_s3_ray.py
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
