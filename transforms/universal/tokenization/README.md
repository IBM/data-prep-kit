# Tokenization Transform 
The tokenization transform annotates pyarrow tables and parquet files
to add a column containing tokens for the document column. 
Per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the core python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the python-based transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
the transform in a kubernetes cluster using a generated `yaml` file.
