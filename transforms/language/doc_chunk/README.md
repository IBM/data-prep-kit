# chunk documents Transform 
This transform is chunking documents from their JSON representation to a list of chunks.
It relies on documents converted with the Docling library in the [pdf2parquet transform](../pdf2parquet).


* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
