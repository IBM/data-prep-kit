# Data Processing Library
The Data Processing Framework is python-based library and set of transforms that enables the 
application of "transforms" to data files (currently parquet) across a distributed 
[Ray](https://docs.ray.io/en/latest/index.html) cluster, enabling the efficient and scalable processing/transformation of virtually 
unlimited amounts of data. 

The framework allows simple 1:1 transformation of parquet files, but also enables
more complex transformations requiring coordination among transforming nodes.
This might include operations such as de-duplification, merging, and splitting.
Some of the available transforms include the following:

* Universal - transforms applicable across code and language model data.
    * [Schema modification](transforms/universal/schema_modification/README.md)
    * [Deduplication (exact)](transforms/universal/ededup/README.md)
    * [Deduplication (fuzzy)](transforms/universal/fdedup/README.md)
    * [Coalesce/split](transforms/universal/coalesce/README.md)
    * [Block listing](transforms/universal/blocklisting/README.md) (based on source url)
* Language - language model specific transforms
    * [Document quality](transforms/universal/doc_quality/README.md)
    * [Language Identification](transforms/universal/language_id/README.md)
* Code - programming language specific transforms.

For more details see [programming model and library](data-processing-lib/doc/overview.md) for details.

# Repository structure
* data_processing_lib - provides a library and framework supporting data transformations in a ray cluster
* kfp - Kubeflow pipeline support
* kind - kind
* transform
    * universal
        * ededup 
        * fdedup 
        * ...
    * code
        * ...
    * language
        * language_id
        * ...

# Build and Makefiles
Makefiles are used for operations performed across all projects in the directory tree.
Using specific rules from the top of the repository tree will recurse their execution
into subdirectories  until subdirectories provide a Makefile that implements the action
and/or recurses further.  For example,
```shell
make build
```
will apply the `make build` rule into all sub-directories supporting such recursion.

Standard rules include the following:

* clean
* build
* test
* publish
* ... 
