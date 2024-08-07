# Chunk documents Transform 

This transform is chunking documents. It supports multiple _chunker modules_.
More details as well as a description of the parameters can be found in the [python/README.md](python/README.md).


* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
