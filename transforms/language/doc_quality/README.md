# Document Quality Transform 
The Document Quality transforms serves as a simple exemplar to demonstrate the development
of a simple 1:1 transform.  Per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [kfp](kfp_ray/README.md) - enables running the ray docker image for
document quality in a kubernetes cluster using a generated `yaml` file.
