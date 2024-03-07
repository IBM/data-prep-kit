# Coalesce files

This is a fairly simple transformer that is converting small files into larger ones by buffering them in memory
until the size of the buffer is not smaller then the required size and then writes the aggregated table out. This
transformer is leveraging flush support in the framework to ensure that no data is left in memory (buffer) once 
processing is completed

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build to build it
```

## Driver options

In addition to the "standard" options described
[here](../../../data-processing-lib/doc/launcher-options.md) transformer defines the following additional parameters:

```shell
  "coalesce_target": 100,
```
Here coalesce target is required size of coalesced table on disk (S3)

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/coalesce_local.py), [s3](src/coalesce_s3.py) and [lakehouse](src/coalesce_lakehouse.py)

# Release notes

Note that coalesce size is specified in MB and refers to the size on disk/in S3. We are assuming there that the memory
size is roughly twice the size on disk

Also note, that as a result of flushing we can have a set of smaller files (the ones that were not combined with 
anything or partially combined) the amount of such smaller files is roughly equal to the amount of workers