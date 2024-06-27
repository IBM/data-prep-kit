# DOC Id Transform 
The doc_id transforms allows the addition of document identifiers, both unique integers and content hashes, 
per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime
* [spark](spark/README.md) - enables the running of a spark-based transformation
in a Spark runtime. 
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.
