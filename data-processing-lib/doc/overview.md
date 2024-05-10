# Data Processing Overview 
The Data Processing Framework is python-based and enables the 
application of "transforms" to data files (currently parquet) across a distributed 
[Ray](https://docs.ray.io/en/latest/index.html) cluster, enabling the
scalable processing/transformation of virtually unlimited amounts of data. 

The framework allows simple 1:1 transformation of parquet files, but also enables
more complex transformations requiring coordination among transforming nodes.
This might include operations such as de-duplication, merging, and splitting.
The framework uses a plugin-model for the primary functions.  The key ones for
developers of data transformation are:

* [Transformation](../src/data_processing/transform/table_transform.py) - a simple, easily-implemented interface defines
the specifics of a given data transformation.
* [Transform Configuration](../src/data_processing/runtime/ray/transform_runtime.py) - defines
the transform short name, its implementation class,  and command line configuration
parameters.

To learn more consider the following:

* [Transform Tutorials](transform-tutorials.md)
* [Transform Runtimes](transform-runtimes.md)
* [Transform Examples](transform-tutorial-examples.md)
* [Testing Transforms](transform-testing.md)
* [Utilities](transformer-utilities.md)
* [Architecture Deep Dive](architecture.md)
* [Transform project root readme](../../transforms/README.md)

