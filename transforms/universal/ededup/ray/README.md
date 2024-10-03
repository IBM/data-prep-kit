# Exact Dedup

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Additional parameters

In addition to [common](../README.md) ededup parameters Ray implementation provides two additional ones

* _hash_cpu_ - specifies amount of CPU per hash actor
* _num_hashes_ - specifies number of hash actors

## ådditional support

We also provide an [estimate](src/cluster_estimator.py) to roughly determine cluster size for running transformer.

## Running

### Launched Command Line Options
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to
[the options provided by the launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).

```
  --ededup_hash_cpu EDEDUP_HASH_CPU
                        number of CPUs per hash
  --ededup_num_hashes EDEDUP_NUM_HASHES
                        number of hash actors to use
  --ededup_doc_column EDEDUP_DOC_COLUMN
                        name of the column containing document
  --ededup_doc_id_column EDEDUP_DOC_ID_COLUMN
                        name of the column containing document id
  --ededup_use_snapshot EDEDUP_USE_SNAPSHOT
                        flag to continue from snapshot
  --ededup_snapshot_directory EDEDUP_SNAPSHOT_DIRECTORY
                        location of snapshot files                      
 ```

These correspond to the configuration keys described above.
