# Coalesce files

Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

This is a fairly simple transformer that is converting small files into larger ones by buffering them in memory
until the size of the buffer is not smaller then the required size and then writes the aggregated table out. This
transformer is leveraging flush support in the framework to ensure that no data is left in memory (buffer) once 
processing is completed

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build to build it
```

## Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py)
configuration for values are as follows:

* _coalesce_target_mb_ - specifies coalesce target in MB

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/coalesce_local_ray.py), [s3](src/coalesce_s3.py) and [lakehouse](src/coalesce_lakehouse.py)

# Release notes

Note that coalesce size is specified in MB and refers to the size on disk/in S3. We are assuming there that the memory
size is roughly twice the size on disk

Also note, that as a result of flushing we can have a set of smaller files (the ones that were not combined with 
anything or partially combined) the amount of such smaller files is roughly equal to the amount of workers
