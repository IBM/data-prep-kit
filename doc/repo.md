# Repository Structure and Use 


# Setup
After cloning the repo, enable the pre-commit hooks.
* Install [pre-commit](https://pre-commit.com/)
* `cd fm-data-engineering`
* `pre-commit install`

# Build
The repository uses Makefiles to manage the build processes.
For example, to build and publish the all transform images
```
cd transforms
make image publish
```
This recursively applies the make targets through the sub-directory tree of transforms to create and publish all transform images.


# Repository structure
* data_processing_lib - provides a library and framework supporting data transformations in a Ray cluster
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
make test 
```
will apply the `make test` rule into all sub-directories supporting such recursion.

Standard rules include the following:

* clean
* setup
* build
* test
* publish
* ... 
