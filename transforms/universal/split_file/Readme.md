# Split  files

This is a fairly simple transformer that is converting large files into smaller ones ones by splitting original
tables into smaller ones we currently support two tipes of splitting:
* splitting based on memory
* splitting based on the amount of documents

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build to build it
```

## Driver options

In addition to the "standard" options described
[here](../../../data-processing-lib/doc/launcher-options.md) transformer defines the following additional parameters:

```shell
    "max_table_size": 1,
    "max_documents_table": 150
```
Here max table size is required size of coalesced table on disk (S3) and max document table is maximum documents
per table

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/split_file_local.py), [s3](src/split_file_s3.py) and [lakehouse](src/split_file_lakehouse.py)

# Release notes

Only one split type is supported for a given run, if both `max_table_size` and `max_documents_table` are specified, the 
error will be thrown and execution will abort
This transformer creates more files then the input. It will create files preserving original name plus integer index 
appended to it
Note that max table size is specified in MB and refers to the size on disk/in S3. We are assuming there that the memory
size is roughly twice the size on disk

Also note, that as a result of flushing we can have a set of smaller files (the ones that were not combined with 
anything or partially combined) the amount of such smaller files is roughly equal to the amount of workers