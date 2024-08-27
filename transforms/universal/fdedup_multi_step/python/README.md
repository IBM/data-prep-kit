# Multistep Fuzzy dedup Python Transform 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

Also see [here](../README.md) on details of implementation

## Summary 
This is a python version of multi step fuzzy dedup

## Configuration and command line Options

Each of the steps has its own configuration options

### Preprocessor

* _doc_column_ - specifies name of the column containing documents (default "contents")
* _doc_id_column_ - specifies the name of the column containing a document id (default "int_document_id")
* _num_permutations_ - specifies number of permutations for fuzzy dedup (default 64)
* _threshold_ -specifies threshold for Jaccardi distance comparison (default .8)
* _shingles_size_ - specifies number of worgs per shingle (default 5)
* _delimiter_ - specifies delimiter for word splitting(default "")
* _use_snapshot_ - specifies flag to continue from snapshot (default False)

### Bucket processor

* _num_permutations_ - specifies number of permutations for fuzzy dedup (default 64. Has to be the same as in preprocessing)
* _threshold_ -specifies threshold for Jaccardi distance comparison (default .8. Has to be the same as in preprocessing)
* _minhash_snapshot_directory_ - specifies directory for minhash snapshots (default None. in this case use "output/snapshot/minhash")

### Filter

* _doc_column_ - specifies name of the column containing documents (default "contents". Has to be the same as in preprocessing)
* _doc_id_column_ - specifies the name of the column containing a document id (default "int_document_id". Has to be the same as in preprocessing)
* _cluster_column_ - specifies the name of the `cluster` column (default "cluster")
* _removed_docs_column_ - specifies the name of the `removed docs` column (default "removed". Note that only the first row has values, the rest are empty arrays)
* _docid_snapshot_directory_ - specifies directory for doc_id snapshots (default None. in this case use "output/snapshot/docs")


## Launch Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
They are different for different steps'

### Preprocessor

```
  --fdedup_preprocessor_doc_column FDEDUP_PREPROCESSOR_DOC_COLUMN
                        document column name
  --fdedup_preprocessor_doc_id_column FDEDUP_PREPROCESSOR_DOC_ID_COLUMN
                        integer document id column name
  --fdedup_preprocessor_num_permutations FDEDUP_PREPROCESSOR_NUM_PERMUTATIONS
                        number of permutations
  --fdedup_preprocessor_threshold FDEDUP_PREPROCESSOR_THRESHOLD
                        threshold
  --fdedup_preprocessor_shingles_size FDEDUP_PREPROCESSOR_SHINGLES_SIZE
                        number of words in shingle
  --fdedup_preprocessor_delimiter FDEDUP_PREPROCESSOR_DELIMITER
                        delimiter for splitting document
  --fdedup_preprocessor_use_snapshot FDEDUP_PREPROCESSOR_USE_SNAPSHOT
                        flag to continue from snapshot
```

### Bucket processor

```
  --fdedup_bucket_processor_num_permutations FDEDUP_BUCKET_PROCESSOR_NUM_PERMUTATIONS
                        number of permutations
  --fdedup_bucket_processor_threshold FDEDUP_BUCKET_PROCESSOR_THRESHOLD
                        threshold
  ----fdedup_bucket_processor_minhash_snapshot_directory FDEDUP_BUCKET_PROCESSOR_MINHASH_SNAPSHOT_DIRECTORY
                        location of minhash snapshot files
```

### Filter

```
  --fdedup_filter_doc_column FDEDUP_FILTER_DOC_COLUMN
                        document column name
  --fdedup_filter_doc_id_column FDEDUP_FILTER_DOC_ID_COLUMN
                        integer document id column name
  ----fdedup_filter_cluster_column FDEDUP_FILTER_CLUSTER_COLUMN
                        cluster column name
  ----fdedup_filter_removed_docs_column FDEDUP_FILTER_REMOVED_DOCS_COLUMN
                        removed documents column name
  ----fdedup_filter_docid_snapshot_directory FDEDUP_FILTER_DOCID_SNAPSHOT_DIRECTORY
                        ID snapshot directory
```

These correspond to the configuration keys described above.

## Running images

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
