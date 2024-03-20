# Data Processing Library
The Data Processing Framework is python-based library and set of transforms that enables the 
transformation, annotation and filtering of data (typically LLM training data contained in 
[parquet](https://arrow.apache.org/docs/python/parquet.html) files).
The distributed infrastructure, based on 
[Ray](https://docs.ray.io/en/latest/index.html), is used to scale out the transformation process
and includes support for running in 
[Kind](https://kind.sigs.k8s.io/) and [KFP (V1)](https://www.kubeflow.org/docs/components/pipelines/v1/).

The framework allows simple 1:1 transformation of parquet files, but also enables
more complex transformations requiring coordination among transforming nodes.
This might include operations such as de-duplification, merging, and splitting.

Topics to explorer
   * [Data schema and processing](data-processing.md)
   * [Available Transforms](transforms/README.md)
   * [Core library](data-processing-lib/README.md) and [its documentation](data-processing-lib/doc/overview.md)

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
        * select_language
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
