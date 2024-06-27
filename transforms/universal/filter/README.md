# Filter Transform 
The filter transforms provides SQL-based expressions for filtering rows and optionally column removal from parquet files, 
per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the python-based transformation
in a Ray runtime
* [spark](spark/README.md) - enables the running of a spark-based transformation
in a Spark runtime. 
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
