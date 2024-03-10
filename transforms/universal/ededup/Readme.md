# Exact Dedup

Please see the set of
[transform project conventions](../../transform-conventions.md)
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

[Transform runtime](src/ededup_transform.py) is responsible for creation hash actors and sending their 
handles to the transforms themselves
Additionally it enhances statistics information with the information about hashes cache size and utilization

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build to build it
```

## Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py)
configuration for values are as follows:

* _hash_cpu_ - specifies an amount of CPUs per hash actor
* _num_hashes_ - specifies number of hash actors
* _doc_column_ - specifies name of the column containing documents

We also provide an [estimate](src/cluster_estimator.py) to roughly determine cluster size for running transformer.

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/ededup_local.py), [s3](src/ededup_s3.py) and [lakehouse](src/ededup_lakehouse.py)

# Release notes