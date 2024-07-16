# Programming Language Selection Transform 
The Programming Language Selection Transform 
annotates input parquet files to add a True/False column indicating if the row's language matches
one of those specified in the transform configuration.
Per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp_ray](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
