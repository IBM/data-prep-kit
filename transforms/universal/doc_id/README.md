# Doc ID Transform 
The Document ID transforms adds a document identification (unique integers and content hashes), which later can be used in de-duplication operations,
per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [spark](spark/README.md) - enables the running of a spark-based transformation
in a Spark runtime. 
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
