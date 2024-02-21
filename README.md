# fm-data-engineering
Opensource version of our data acquisition and processing modules and pipelines, across Code and Language.

# Repository structure
* data_processing_lib - provides a library and framework supporting data transformations in a ray cluster
* transform
    * universal
        * ededup 
        * fdedup 
        * resize
        * tokenization 
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
