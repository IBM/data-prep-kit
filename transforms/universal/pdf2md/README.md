# PDF2MD Transform 


The PDF2MD transforms iterate through PDF files or zip of PDF files and generates parquet files
containing the converted document in Markdown format.

The PDF conversion is using the [Docling package](https://github.com/DS4SD/docling).

The following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
