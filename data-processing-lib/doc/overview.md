# Data Processing Overview 
The Data Processing Framework is python-based and enables the 
application of "transforms" to data files (currently parquet) across a distributed 
[Ray](https://docs.ray.io/en/latest/index.html) cluster, enabling the
scalable processing/transformation of virtually unlimited amounts of data. 

The framework allows simple 1:1 transformation of parquet files, but also enables
more complex transformations requiring coordination among transforming nodes.
This might include operations such as de-duplification, merging, and splitting.
The framework uses a plugin-model for the primary functions.  The key ones for
developers of data transformation are:
* [Transformation](../src/data_processing/transform/table_transform.py) - a simple, easily-implemented interface defines
the requirements for a data transform.
* [Transformation Runtime](../src/data_processing/ray/transform_runtime.py) - allows for customization of the environment for the transformer.
This might include provisioning of shared memory objects or creation of additional actors.

To learn more consider the following:
* [Architecture Deep Dive](architecture.md)
* [Transform Development Environment](transform-dev-env.md)
* [Transform Tutorials](transform-tutorials.md)

