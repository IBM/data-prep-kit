# Spark Framework 
The Spark runtime implementation is roughly based on the ideas from 
[here](https://wrightturn.wordpress.com/2015/07/22/getting-spark-data-from-aws-s3-using-boto-and-pyspark/),
[here](https://medium.com/how-to-become-a-data-architect/get-best-performance-for-pyspark-jobs-using-parallelize-48c8fa03a21e)
and [here](https://medium.com/@shuklaprashant9264/alternate-of-for-loop-in-pyspark-25a00888ec35). 
Spark itself is basically used for execution parallelization, but all data access is based on the
framework's [data access](data-access-factory.md), thus preserving all the implemented features. At 
the start of the execution, the list of files to process is obtained (using data access framework)
and then split between Spark workers for reading actual data, its transformation and writing it back.
The implementation is based on Spark RDD (For comparison of the three Apache Spark APIs: 
RDDs, DataFrames, and Datasets see this 
[Databricks blog post](https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html))
As defined by Databricks:
```text
RDD was the primary user-facing API in Spark since its inception. At the core, an RDD is an 
immutable distributed collection of elements of your data, partitioned across nodes in your 
cluster that can be operated in parallel with a low-level API that offers transformations 
and actions.
```
This APIs fits perfectly into what we are implementing. It allows us to fully leverage our 
existing DataAccess APIs thus preserving all of the investments into flexible, reliable data 
access. Additionally RDDs flexible low-level control allows us to work on partition level, 
thus limiting the amount of initialization and set up.
Note that in our approach transform's processing is based on either binary or parquet data, 
not Spark DataFrames or DataSet. We are not currently supporting supporting these Spark APIs, 
as they are not well mapped into what we are implementing.

In our implementation we are using 
[pyspark.SparkContext.parallelize](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.parallelize.html)
for running multiple transforms in parallel. We allow 2 options for specifying the number of partitions, determining 
how many partitions the RDD should be divided into. See
[here](https://sparktpoint.com/how-to-create-rdd-using-parallelize/) for the explanation
of this parameter:
* If you specify a positive value of the parameter, Spark will attempt to evenly
  distribute the data from seq into that many partitions. For example, if you have
  a collection of 100 elements and you specify numSlices as 4, Spark will try
  to create 4 partitions with approximately 25 elements in each partition. 
* If you don’t specify this parameter, Spark will use a default value, which is
  typically determined based on the cluster configuration or the available resources
  (number of workers).

## Transforms

* [SparkTransformRuntimeConfiguration](../spark/src/data_processing_spark/runtime/spark/runtime_configuration.py)
  allows to configure transform to use PySpark. In addition to its base class
  [TransformRuntimeConfiguration](../python//src/data_processing/runtime/runtime_configuration.py) features,
  this class includes `get_bcast_params()` method to get very large configuration settings. Before starting the
  transform execution, the Spark runtime will broadcast these settings to all the workers.

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