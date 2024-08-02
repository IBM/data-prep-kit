# Issues Transform 
This issues transform is designed to filter issues.
Per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation and python runtime.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp_ray](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
