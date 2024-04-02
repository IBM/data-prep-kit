# Split  files

Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

This is a simple transformer that is resizing the input tables to a specified size. 
* resizing based on in-memory size of the tables.
* resized based on the number of rows in the tables. 

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build 
```

## Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py)
configuration for values are as follows:

* _max_rows_per_table - specifies max size of table on disk/S3
* _max_documents_table_ - specifies max documents per table

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/resize_local_ray.py), [s3](src/resize_s3.py) and [lakehouse](src/resize_lakehouse.py)

# Release notes

Only one split type is supported for a given run, if both `max_table_size` and `max_documents_table` are specified, the 
error will be thrown and execution will abort
This transformer creates more files then the input. It will create files preserving original name plus integer index 
appended to it
Note that max table size is specified in MB and refers to the size on disk/in S3. We are assuming there that the memory
size is roughly twice the size on disk

Also note, that as a result of flushing we can have a set of smaller files (the ones that were not combined with 
anything or partially combined) the amount of such smaller files is roughly equal to the amount of workers