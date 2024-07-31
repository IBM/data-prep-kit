# Text encoder Transform 

This transform is using sentence encoder models to create embeddings of the text in each row of the input .parquet table.

Per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
