# Fuzzy Deduplification Transform 
The fdedup transforms removes documents that are very similar to each other within a set of parquet files, 
per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
