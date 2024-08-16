# Spark Framework 
The Spark runtime extends the base framework with the following set of components:

## Transforms

* [AbstractSparkTransform](../spark/src/data_processing_spark/runtime/spark/spark_transform.py) - this
  is the base class for all spark-based transforms over spark DataFrames.
* [SparkTransformConfiguration](../spark/src/data_processing_spark/runtime/spark/spark_transform_config.py) - this
  is simple extension of the base  TransformConfiguration class to hold the transformation class
  (an extension of AbstractSparkTransform).

## Runtime

* [SparkTransformLauncher](../spark/src/data_processing_spark/runtime/spark/spark_launcher.py) - this is a 
class generally used to implement `main()` that makes use of a `SparkTransformConfiguration` to 
start the Spark runtime and execute the transform over the specified set of input files.
* [SparkTransformRuntimeConfiguration](../spark/src/data_processing_spark/runtime/spark/runtime_config.py) - this 
class is a simple extension of the transform's base TransformConfiguration class. 
