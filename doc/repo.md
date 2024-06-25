# Repository Structure and Use 

Here we discuss the structure, use and approach to code management in the repo.

# Repository structure
* data_processing_lib - provides the core transform framework and library 
supporting data transformations in 3 runtimes
    * python 
    * ray
    * spark
 
* transform
    * universal
        * noop 
          * python 
          * ray
          * spark 
          * kfp_ray
        * ...
    * code
        * code_quality
            * ray
            * kfp_ray
        * ...
    * language
        * ...
* kfp - Kubeflow pipeline support
    * kfp_support_lib - Data Preparation Kit Library. KFP support
    * kfp_ray_components - Kubflow pipeline components used in the pipelines
* scripts


# Build and Makefiles
Makefiles are used for operations performed across all projects in the directory tree.
There are two types of users envisioned to use the make files.  

* adminstrators - perform git actions and release management 
* developers - work with core libraries and transforms

Each directory has access to a `make help` target that will show all available targets.

## Administrators 
Generally, administrators will issue make commands from the top of the repository to, for example
publish a new release.  The top level make file provides a set of targets that 
are executed recursively, which as a result are expected to be implementd by
sub-directories.  These and their semantics are expected to be implemented,
as appropriate, in the sub-directories are as follows:

* clean - Restore the directory to as close to initial repository clone state as possible. 
* build - Build all components contained in a given sub-directory.  
This might include pypi distributions, images, etc.
* test -  Test all components contained in a given sub-directory. 
* publish - Publish any components in sub-directory. 
This might include things published to pypi or the docker registry.
* set-versions - apply the DPK_VERSION to all published components. 

Sub-directories are free to define these as empty/no-op targets, but generally are required
to define them unless a parent directory does not recurse into the directory.

## Developers
Generally, developers will be working in a python project directory
(e.g., data-processing-lib/python, transforms/universal/filter, etc.) 
and can issue the administrator's make targets (e g., build, test, etc)
or others that might be defined locally
(e.g., venv, test-image, test-src in transform projects).
Key targets are as follows:

* venv -  creates the virtual environment from either a pyproject.toml or requirements.txt file.
* publish - publish libraries or docker images as appropriate.  
This is generally only used during release generation.
 
If working with an IDE, one generally makes the venv, then configures the IDE to 
reference the venv, src and test directories.

Transform projects generally include these transform project-specific targets for convenience,
which are triggered with the the `test` target.

* test-src - test python tests in the test directory
* test-image - build and test the docker image for the transform

Please also consult [transform project conventions](../transforms/README.md#transform-project-conventions) for 
additional considerations when developing transforms.

### Transforms and KFP 
The kfp_ray directories in the transform projects provide 
`workflow-` targets and are dedicated to handling the 
[Kubeflow Pipelines](https://github.com/kubeflow/pipelines) 
workflows for the specified transforms.

```

