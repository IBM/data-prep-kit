# Profiler

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

Profiler implement a word count. Typical implementation of the word count is done using map reduce.
* It’s O(N2) complexity
* shuffling with lots of data movement

Implementation here is using “streaming” aggregation, based on central cache:

* At the heart of the implementation is a cache of partial word counts, implemented as a set of Ray actors and containing 
word counts processed so far.
* Individual data processors are responsible for:
  * Reading data from data plane
  * tokenizing documents (we use pluggable tokenizer)
  * Coordinating with distributed cache to collect overall word counts

The complication of mapping this model to transform model is the fact that implementation requires an aggregators cache, 
that transform mode knows nothing about. The solution here is to use transform runtime to create cache
and pass it as a parameter to transforms.

## Transform runtime

[Transform runtime](src/profiler_transform_ray.py) is responsible for creation cache actors and sending their 
handles to the transforms themselves
Additionally it writes created word counts to the data storage (as .csv files) and enhances statistics information with the information about cache size and utilization

## Configuration and command line Options

The set of dictionary keys holding [EdedupTransform](src/profiler_transform_ray.py)
configuration for values are as follows:

* _aggregator_cpu_ - specifies an amount of CPUs per aggregator actor
* _num_aggregators_ - specifies number of aggregator actors
* _doc_column_ - specifies name of the column containing documents

## Running

### Launched Command Line Options
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to
[the options provided by the launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).

```shell
  --profiler_aggregator_cpu PROFILER_AGGREGATOR_CPU
                        number of CPUs per aggrigator
  --profiler_num_aggregators PROFILER_NUM_AGGREGATORS
                        number of agregator actors to use
  --profiler_doc_column PROFILER_DOC_COLUMN
                        key for accessing data
 ```

These correspond to the configuration keys described above.

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
