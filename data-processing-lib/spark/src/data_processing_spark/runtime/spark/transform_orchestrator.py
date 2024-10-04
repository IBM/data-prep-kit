# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import time
import traceback
from datetime import datetime

from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics
from data_processing.utils import GB, get_logger
from data_processing_spark.runtime.spark import (
    SparkTransformFileProcessor,
    SparkTransformRuntimeConfiguration,
    SparkTransformExecutionConfiguration,
)
from pyspark import SparkConf, SparkContext


logger = get_logger(__name__)


def orchestrate(
    runtime_config: SparkTransformRuntimeConfiguration,
    execution_configuration: SparkTransformExecutionConfiguration,
    data_access_factory: DataAccessFactoryBase,
) -> int:
    """
    orchestrator for transformer execution
    :param data_access_factory: data access factory
    :param runtime_config: transformer runtime configuration
    :param execution_configuration: orchestrator configuration
    :return: 0 - success or 1 - failure
    """
    start_time = time.time()
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"orchestrator started at {start_ts}")
    # create data access
    data_access = data_access_factory.create_data_access()
    if data_access is None:
        logger.error("No DataAccess instance provided - exiting")
        return 1
    # initialize Spark
    conf = SparkConf().setAppName(runtime_config.get_name()).set("spark.driver.host", "127.0.0.1")
    sc = SparkContext(conf=conf)
    spark_runtime_config = sc.broadcast(runtime_config)
    daf = sc.broadcast(data_access_factory)

    def process_partition(iterator):
        """
        process partitions
        :param iterator: iterator of records
        :return:
        """
        # local statistics dictionary
        statistics = TransformStatistics()
        # create transformer runtime
        d_access_factory = daf.value
        runtime_conf = spark_runtime_config.value
        runtime = runtime_conf.create_transform_runtime()
        # create file processor
        file_processor = SparkTransformFileProcessor(
            data_access_factory=d_access_factory, runtime_configuration=runtime_conf, statistics=statistics
        )
        first = True
        for f in iterator:
            # for every file
            if first:
                logger.debug(f"partition {f}")
                # add additional parameters
                transform_params = (
                    runtime.get_transform_config(partition=int(f[1]), data_access_factory=d_access_factory,
                                                 statistics=statistics))
                # create transform with partition number
                file_processor.create_transform(transform_params)
                first = False
            # process file
            file_processor.process_file(f_name=f[0])
        # flush
        file_processor.flush()
        # enhance statistics
        runtime.compute_execution_stats(statistics)
        # return partition's statistics
        return list(statistics.get_execution_stats().items())

    num_partitions = 0
    try:
        # Get files to process
        files, profile, retries = data_access.get_files_to_process()
        if len(files) == 0:
            logger.error("No input files to process - exiting")
            return 0
        logger.info(f"Number of files is {len(files)}, source profile {profile}")
        # process data
        logger.debug("Begin processing files")
        # process files split by partitions
        logger.debug(f"parallelization {execution_configuration.parallelization}")
        if execution_configuration.parallelization > 0:
            source_rdd = sc.parallelize(files, execution_configuration.parallelization)
        else:
            source_rdd = sc.parallelize(files)
        num_partitions = source_rdd.getNumPartitions()
        logger.info(f"Parallelizing execution. Using {num_partitions} partitions")
        stats_rdd = source_rdd.zipWithIndex().mapPartitions(process_partition)
        # build overall statistics
        stats = dict(stats_rdd.reduceByKey(lambda a, b: a + b).collect())
        return_code = 0
        status = "success"
    except Exception as e:
        # process execution exception
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return_code = 1
        status = "failure"
        stats = {}
    try:
        # build and save metadata
        logger.debug("Building job metadata")
        cpus = sc.defaultParallelism
        executors = sc._jsc.sc().getExecutorMemoryStatus()
        memory = 0.0
        for i in range(executors.size()):
            memory += executors.toList().apply(i)._2()._1()
        resources = {"cpus": cpus, "gpus": 0, "memory": round(memory/GB, 2), "object_store": 0}
        input_params = runtime_config.get_transform_metadata() | execution_configuration.get_input_params()
        metadata = {
            "pipeline": execution_configuration.pipeline_id,
            "job details": execution_configuration.job_details
            | {
                "start_time": start_ts,
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
            },
            "code": execution_configuration.code_location,
            "job_input_params": input_params | data_access_factory.get_input_params(),
            "execution_stats": {
                "num partitions": num_partitions,
                "execution time, min": round((time.time() - start_time) / 60, 3),
            } | resources,
            "job_output_stats": stats,
        }
        logger.debug(f"Saving job metadata: {metadata}.")
        data_access.save_job_metadata(metadata)
        logger.debug("Saved job metadata.")
        return return_code
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return 1
    finally:
        # stop spark context at the end. Required for running multiple tests
        sc.stop()
