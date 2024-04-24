# Fuzzy Dedup

Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

The basic implementation of the fuzzy dedup is based on [MinHash](https://en.wikipedia.org/wiki/MinHash). Also see
[here](http://infolab.stanford.edu/~ullman/mmds/ch3n.pdf) for more details. The architecture of the implementation is presented here:

![](images/fuzzy.png)

The main components of implementation are driver, processors (implemented as actor pools) - table processor, table 
filter and bucket hash processor, and hash actors - minhash, buckets and docs. 

The complication of mapping this model to transform model is the fact that in this model assumes a two pass processing, 
while a transform model is a single pass. The solution to this mismatch is to use transform runtime to implement the 
first path and use the native transform pipeline to implement filtering.

## Transform runtime
The [transform runtime](src/fdedup_transform.py) is implementing complete first path of the fuzzy deduping:
* creates bucket and minhash collectors
* implements initial file processing to populate bucket and minhash caches
* creates doc collectors 
* implement bucket processing
* Clean up everything except for doc collectors in preparation to filter, that is implemented by the framework proper
The main components of runtime are described below

### TableProcessor Actor

[Table processing actor](src/fdedup_transform.py) is implemented following framework itself is implemented as a pair -
`FdedupTransform` implementing the actual transformation and and 
[transform table processor](../../../data-processing-lib/src/data_processing/ray/transform_table_processor.py) 
(from the framework itself).

### DocsMinHash Actor

This [actor](src/fdedup_support.py) stores MInHashes

### BucketsHash Actor

This actor [actor](src/fdedup_support.py)

### BucketHashProcessor

BucketHash [actor](src/fdedup_support.py) implement the actual buckets processing, removing duplicates. 
Implementation of this actor allows to better manage this "expensive" process, by using Actor pool load balancing
thus minimizing overall time for this operation. Instead of pre partitioning buckets, it is using dynamic load
partitioning. We also are processing "longest" buckets first thus further improving performance. To further improve
the overall performance we can in future implement bucket splitting - its faster to process more smaller buckets 
then the long ones

### BucketHashProcessor

This [actor](src/fdedup_support.py) is queueing up requests to the `BucketHashProcessor` actor pool, which load 
balances their execution

### DocCollector Actor

This [actor](src/fdedup_support.py) is a collector for unique documents

## Transformer

In the fuzzy dedup implementation, the [transformer](src/fdedup_transform.py) only implements filtering. For every
table, it checks document ids with the `DocumentsCollector` cache and removes all of the rows which do not have ids in 
the hash 

## Snapshotting

Fuzzy dedup often runs on very large data sets and implements three very distinct phases:
* Building buckets
* Processing buckets
* Filtering data
To improve recoverability of fuzzy dedup, current implementation includes snapshotting - at the end of the first two 
phases we snapshot the current state of execution - bucket and minhash actors after the first phase and document actors 
after the second. This snapshotting provide code with the ability to restart from the existing snapshot. You can use one
of two configuration flags (assuming snapshots exist):
* `use_bucket_snapshot` to start from the second phase
* `use_doc_snapshot` to start from the third phase

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use 

```shell
make build to build it
```

### Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py)
configuration for values are as follows:

* _bucket_cpu_ - specifies number of CPUs for bucket actor
* _doc_cpu_ - specifies number of CPUs for doc actor
* _mhash_cpu_ - specifies number of CPUs for minhash actor
* _num_doc_actors_ - specifies number of doc actors
* _num_bucket_actors_ - specifies number of bucket actors
* _num_minhash_actors_ - specifies number of minhash actors
* _num_preprocessors_ - specifies number of preprocessors
* _num_permutations_ - specifies number of permutations
* _threshold_ - specifies threshold
* _shingles_size_ - specifies shingles size
* _japanese_data_ - specifies whether to use japanese specific document splitting
* _delimiters_ - specifies delimiter for non japanese document splitting
* _snapshot_delay_ - delay between different actors reading/writing snapshot not to overwhelm storage
* -use_bucket_snapshot_ - run from the existing buckets snapshot (bypass building buckets)
* -use_doc_snapshot_ - run from the existing docs snapshot (bypass building and processing buckets)

Above you see both parameters and their values for small runs (tens of files). We also provide an 
[estimate](src/cluster_estimator.py) to roughly determine cluster size for running transformer.

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/fdedup_local_ray.py) and [s3](src/fdedup_s3_ray.py)

```shell
$ source venv/bin/activate
(venv) $ export PYTHONPATH=src
(venv) $ python src/fdedup_local_ray.py
15:12:17 INFO - Running locally
15:12:17 INFO - fuzzy dedup params are {'doc_column': 'contents', 'id_column': 'int_id_column', 'cluster_column': 'cluster', 'worker_options': {'num_cpus': 0.8}, 'bucket_cpu': 0.5, 'doc_cpu': 0.5, 'mhash_cpu': 0.5, 'd_actors': 2, 'b_actors': 1, 'm_actors': 1, 'n_preprocessors': 2, 'num_permutations': 64, 'threshold': 0.8, 'world_shingle_size': 5, 'delimiters': ' ', 'random_delay_limit': 5, 'snapshot_delay': 1, 'use_bucket_snapshot': False, 'use_doc_snapshot': False}
15:12:17 INFO - data factory data_ is using local data accessinput_folder - /Users/boris/Projects/data-prep-lab/transforms/universal/fdedup/test-data/input output_folder - /Users/boris/Projects/data-prep-lab/transforms/universal/fdedup/output
15:12:17 INFO - data factory data_ max_files -1, n_sample -1
15:12:17 INFO - data factory data_ Not using data sets, checkpointing False, max files -1, random samples -1, files to use ['.parquet']
15:12:17 INFO - number of workers 3 worker options {'num_cpus': 0.8}
15:12:17 INFO - pipeline id pipeline_id; number workers 3
15:12:17 INFO - job details {'job category': 'preprocessing', 'job name': 'fdedup', 'job type': 'ray', 'job id': 'job_id'}
15:12:17 INFO - code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
15:12:17 INFO - actor creation delay 0
2024-04-18 15:12:19,864	INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
(orchestrate pid=9410) 15:12:22 INFO - orchestrator started at 2024-04-18 15:12:22
(orchestrate pid=9410) 15:12:22 INFO - Number of files is 1, source profile {'max_file_size': 0.03486919403076172, 'min_file_size': 0.03486919403076172, 'total_file_size': 0.03486919403076172}
(orchestrate pid=9410) 15:12:22 INFO - Cluster resources: {'cpus': 16, 'gpus': 0, 'memory': 11.41860160883516, 'object_store': 2.0}
(orchestrate pid=9410) 15:12:22 INFO - Number of workers - 3 with {'num_cpus': 0.8} each
(orchestrate pid=9410) 15:12:22 INFO - starting run from the beginning
(orchestrate pid=9410) 15:12:22 INFO - continuing from the very beginning
(orchestrate pid=9410) 15:12:22 INFO - Fuzzy: num buckets 5, bucket length 11
(orchestrate pid=9410) 15:12:22 INFO - created 1 bucket actors
(orchestrate pid=9410) 15:12:22 INFO - created 1 minhash actors
(orchestrate pid=9410) 15:12:22 INFO - Table preprocessing uses 3 readers
(orchestrate pid=9410) 15:12:22 INFO - created 3 table processor actors
(orchestrate pid=9410) 15:12:22 INFO - Completed 0 files in 6.620089213053385e-06 min. Waiting for completion
(orchestrate pid=9410) 15:12:28 INFO - Completed processing in 0.1015137513478597 min
(orchestrate pid=9410) 15:12:28 INFO - creating minhash snapshots
(orchestrate pid=9410) 15:12:29 INFO - minhash snapshots created
(orchestrate pid=9410) 15:12:29 INFO - creating bucket snapshots
(orchestrate pid=9410) 15:12:30 INFO - bucket snapshots created
(orchestrate pid=9410) 15:12:30 INFO - created 2 document actors
(orchestrate pid=9410) 15:12:30 INFO - created 2 bucket processor actors
(orchestrate pid=9410) 15:12:30 INFO - created bucket processor invoker
(orchestrate pid=9410) 15:12:30 INFO - added invoker to bucket collectors
(BucketsHash pid=9414) 15:12:30 INFO - processing buckets 0 long, 15 short
(BucketsHash pid=9414) 15:12:30 INFO - Done submitting long buckets
(BucketsHashProcessorInvoker pid=9423) 15:12:31 INFO - Waiting bucket processing completion
(orchestrate pid=9410) 15:12:32 INFO - Done processing buckets in 0.024500517050425212 min
(orchestrate pid=9410) 15:12:32 INFO - creating document snapshots
(orchestrate pid=9410) 15:12:34 INFO - document snapshots created
(orchestrate pid=9410) 15:12:34 INFO - Completed 0 files in 9.067853291829427e-06 min. Waiting for completion
(orchestrate pid=9410) 15:12:37 INFO - Completed processing in 0.06522523562113444 min
(orchestrate pid=9410) 15:12:37 INFO - done flushing in 0.002173900604248047 sec
15:12:47 INFO - Completed execution in 0.5121301531791687 min, execution result 0
````

### Launched Command Line Options
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).

```shell
  --fdedup_doc_column FDEDUP_DOC_COLUMN
                        document column name
  --fdedup_id_column FDEDUP_ID_COLUMN
                        integer document id column name
  --fdedup_cluster_column FDEDUP_CLUSTER_COLUMN
                        cluster column name
  --fdedup_bucket_cpu FDEDUP_BUCKET_CPU
                        number of CPUs per bucket hash
  --fdedup_mhash_cpu FDEDUP_MHASH_CPU
                        number of CPUs per minhash hash
  --fdedup_doc_cpu FDEDUP_DOC_CPU
                        number of CPUs per doc hash
  --fdedup_num_doc_actors FDEDUP_NUM_DOC_ACTORS
                        number of doc actors to use
  --fdedup_num_minhash_actors FDEDUP_NUM_MINHASH_ACTORS
                        number of minhash actors to use
  --fdedup_num_bucket_actors FDEDUP_NUM_BUCKET_ACTORS
                        number of bucket actors to use
  --fdedup_num_preprocessors FDEDUP_NUM_PREPROCESSORS
                        number of preprocessors to use
  --fdedup_num_permutations FDEDUP_NUM_PERMUTATIONS
                        number of permutations
  --fdedup_threshold FDEDUP_THRESHOLD
                        threshold
  --fdedup_shingles_size FDEDUP_SHINGLES_SIZE
                        number of words in shingle
  --fdedup_delimiters FDEDUP_DELIMITERS
                        delimiter for splitting document
  --fdedup_snapshot_delay FDEDUP_SNAPSHOT_DELAY
                        snapshot delay time
  --fdedup_use_bucket_snapshot FDEDUP_USE_BUCKET_SNAPSHOT
                        flag to continue with bucket snapshot
  --fdedup_use_doc_snapshot FDEDUP_USE_DOC_SNAPSHOT
                        flag to continue with doc snapshot
  --fdedup_random_delay_limit FDEDUP_RANDOM_DELAY_LIMIT
                        maximum delay between read
```

These correspond to the configuration keys described above.
