# Spark Framework 
The Spark runtime implementation is roughly based on the ideas from 
[here](https://wrightturn.wordpress.com/2015/07/22/getting-spark-data-from-aws-s3-using-boto-and-pyspark/),
[here][https://medium.com/how-to-become-a-data-architect/get-best-performance-for-pyspark-jobs-using-parallelize-48c8fa03a21e].
and [here](https://medium.com/@shuklaprashant9264/alternate-of-for-loop-in-pyspark-25a00888ec35)
Spark itself is basically used for execution parallelization, but all data access is based on the
framework's [data access](data-access-factory.md), thus preserving all the implemented features. At 
the start of the execution, the list of files to process is obtained (using data access framework)
and then split between Spark workers for reading actual data, its transformation and writing it back.

## Transforms

* [SparkTransformRuntimeConfiguration](../spark/src/data_processing_spark/transform/runtime_configuration.py) allows
    to configure transform to use PySpark


## Runtime

Spark runtime extends the base framework with the following set of components:
* [SparkTransformExecutionConfiguration](../spark/src/data_processing_spark/runtime/spark/execution_configuration.py)
  allows to configure Spark execution
* [SparkTransformFileProcessor](../spark/src/data_processing_spark/runtime/spark/transform_file_processor.py) extends
  [AbstractTransformFileProcessor](../python/src/data_processing/runtime/transform_file_processor.py) to work on
  PySpark
* [SparkTransformLauncher](../spark/src/data_processing_spark/runtime/spark/transform_launcher.py) allows
  to launch PySpark runtime and execute a transform
* [orchestrate](../spark/src/data_processing_spark/runtime/spark/transform_orchestrator.py) function orchestrates Spark
  based execution