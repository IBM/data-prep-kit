# Multistep Fuzzy dedup Ray Transform 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

Also see [here](../README.md) on details of implementation

## Summary 
This is a Ray version of multi step fuzzy dedup

## Configuration and command line Options

Each of the steps has its own configuration options. Main options are described [here](../python/README.md).
This document only defines additional parameters specific for Ray

### Preprocessor

* _bucket_cpu_ - specifies number of cpus for bucket hash actor (Default .5)
* _doc_id_cpu_ - specifies number of cpus for doc id hash actor (Default .5)
* _minhash_cpu_ - specifies number of cpus for minhash hash actor (Default .5)
* _num_minhashes_ - specifies number of minhash cache actors to use (Default 1)
* _num_doc_id_ - specifies number of doc id cache actors to use (Default 1)
* _num_buckets_ - specifies number of bucket cache actors to use (Default 1)

### Bucket processor

* _bucket_cpu_ - specifies number of cpus for bucket hash actor (Default .5)
* _doc_id_cpu_ - specifies number of cpus for doc id hash actor (Default .5)
* _minhash_cpu_ - specifies number of cpus for minhash hash actor (Default .5)
* processor_cpu_ - specifies number of cpus for bucket processor actor (Default .8)
* _num_minhashes_ - specifies number of minhash cache actors to use (Default 1)
* _num_doc_id_ - specifies number of doc id cache actors to use (Default 1)
* _num_buckets_ - specifies number of bucket cache actors to use (Default 1)
* _num_processors_ - specifies number of bucket processor actors to use (Default 2)

### Filter

* _doc_id_cpu_ - specifies number of cpus for doc id hash actor (Default .5)
* _num_doc_id_ - specifies number of doc id cache actors to use (Default 1)


## Launch Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [Ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).
They The basic command line options are defined [here](../python/README.md). This document only shows
additional (Ray specific) command line options

### Preprocessor

```
  --fdedup_preprocessor_bucket_cpu FDEDUP_PREPROCESSOR_BUCKET_CPU
                        number of CPUs per bucket hash
  --fdedup_preprocessor_doc_id_cpu FDEDUP_PREPROCESSOR_DOC_ID_CPU
                        number of CPUs per doc id hash
  --fdedup_preprocessor_minhash_cpu FDEDUP_PREPROCESSOR_MINHASH_CPU
                        number of CPUs per minhash hash
  --fdedup_preprocessor_num_minhashes FDEDUP_PREPROCESSOR_NUM_MINHASHES
                        number of minhash caches to use
  --fdedup_preprocessor_num_doc_id FDEDUP_PREPROCESSOR_NUM_DOC_ID
                        number of doc id caches to use
  --fdedup_preprocessor_num_buckets FDEDUP_PREPROCESSOR_NUM_BUCKETS
                        number of bucket hashes to use
```

### Bucket processor

```  
  --fdedup_bucket_processor_bucket_cpu FDEDUP_BUCKET_PROCESSOR_BUCKET_CPU
                        number of CPUs per bucket hash
  --fdedup_bucket_processor_minhash_cpu FDEDUP_BUCKET_PROCESSOR_MINHASH_CPU
                        number of CPUs per minhash hash
  --fdedup_bucket_processor_doc_id_cpu FDEDUP_BUCKET_PROCESSOR_DOC_ID_CPU
                        number of CPUs per docid hash
  --fdedup_bucket_processor_processor_cpu FDEDUP_BUCKET_PROCESSOR_PROCESSOR CPU
                        number of CPUs per bucket processor
  --fdedup_bucket_processor_num_minhashes FDEDUP_BUCKET_PROCESSOR_NUM_MINHASHES
                        number of minhash caches to use
  --fdedup_bucket_processor_num_buckets FDEDUP_BUCKET_PROCESSOR_NUM_BUCKETS
                        number of bucket hashes to use
  --fdedup_bucket_processor_num_doc_id FDEDUP_BUCKET_PROCESSOR_NUM_DOC_ID
                        number of docid hashes to use
  --fdedup_bucket_processor_num_processors FDEDUP_BUCKET_PROCESSOR_NUM_PROCESSORS
                        number of bucket processors to use
```

### Filter

```
  --fdedup_filter_doc_id_cpu FDEDUP_FILTER_DOC_ID_CPU
                        number of CPUs per doc-id hash
  --fdedup_filter_num_doc_id FDEDUP_FILTER_NUM_DOC_ID
                        number of doc id caches to use
```

These correspond to the configuration keys described above.

## Running images

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
