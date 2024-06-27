# Exact Dedup

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

Exact data deduplication is used to identify (and remove) records determined by native documents.
* It’s O(N2) complexity
* shuffling with lots of data movement

It can be implemented using 2 approaches: 
* Exact string matching
* Hash-based matching (ASSUMPTION: a hash is unique to each native document.) – moving hash value is cheaper than moving full content

Implementation here is using “streaming” deduplication, based on central hash:

![](images/exactdedup.png)

* At the heart of the implementation is a hash cache implemented as a set of Ray actors and containing 
unique hashes seen so far.
* Individual data processors are responsible for:
  * Reading data from data plane
  * Converting documents into hashes
  * Coordinating with distributed hashes cache to remove the duplicates
  * Storing unique documents back to the data plane

The complication of mapping this model to transform model is the fact that implementation requires a hash cache, 
that transform mode knows nothing about. The solution here is to use transform runtime to create haches cache.
and pass it as a parameter to transforms.

## Transform runtime

[Transform runtime](src/ededup_transform_ray.py) is responsible for creation hash actors and sending their 
handles to the transforms themselves
Additionally it enhances statistics information with the information about hashes cache size and utilization

## Configuration and command line Options

The set of dictionary keys holding [EdedupTransform](src/ededup_transform_ray.py)
configuration for values are as follows:

* _hash_cpu_ - specifies an amount of CPUs per hash actor
* _num_hashes_ - specifies number of hash actors
* _doc_column_ - specifies name of the column containing documents

We also provide an [estimate](src/cluster_estimator.py) to roughly determine cluster size for running transformer.

## Running

### Launched Command Line Options
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to
[the options provided by the launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).

```shell
  --ededup_hash_cpu EDEDUP_HASH_CPU
                        number of CPUs per hash
  --ededup_num_hashes EDEDUP_NUM_HASHES
                        number of hash actors to use
  --ededup_doc_column EDEDUP_DOC_COLUMN
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
