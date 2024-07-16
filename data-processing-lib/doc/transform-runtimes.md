# Transform Runtimes

Runtimes provide a mechanism to run a transform over a set of input files to produce a set of
output files.  Each runtime is started using a _launcher_.  
The available runtimes are as follows: 

* [Python runtime](python-runtime.md) - provides single process running of a transform.
* [Ray runtime](ray-runtime.md) - provides running transforms across multiple Ray workers to
  achieve highly scalable processing.
* [Spark runtime](spark-runtime.md) - provides running spark-based transforms in a spark cluster.
