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
from data_processing.runtime import TransformRuntimeConfiguration
from data_processing.utils import get_logger
from data_processing_spark.runtime.spark import (
    SparkTransformExecutionConfiguration,
    SparkTransformFileProcessor,
)
from pyspark import SparkConf, SparkContext


logger = get_logger(__name__)


def orchestrate(
    runtime_config: TransformRuntimeConfiguration,
    execution_config: SparkTransformExecutionConfiguration,
    data_access_factory: DataAccessFactoryBase,
) -> int:
    """
    orchestrator for transformer execution
    :param execution_config: orchestrator configuration
    :param data_access_factory: data access factory
    :param runtime_config: transformer runtime configuration
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
    transform_config = sc.broadcast(runtime_config)
    daf = sc.broadcast(data_access_factory)

    def process_partition(iterator):
        """
        process partitions
        :param iterator: iterator of records
        :return:
        """
        # local statistics dictionary
        statistics = {}
        # create file processor
        file_processor = SparkTransformFileProcessor(
            data_access_factory=daf.value, runtime_configuration=transform_config.value, statistics=statistics
        )
        first = True
        for f in iterator:
            # for every file
            if first:
                logger.debug(f"partition {f}")
                # create transform with partition number
                file_processor.create_transform(partition=int(f[1]))
                first = False
            # process file
            file_processor.process_file(f_name=f[0])
        # flush
        file_processor.flush()
        # return partition's statistics
        return list(statistics.items())

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
        logger.debug(f"parallelization {execution_config.parallelization}")
        if execution_config.parallelization > 0:
            source_rdd = sc.parallelize(files, execution_config.parallelization)
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
        input_params = runtime_config.get_transform_metadata() | execution_config.get_input_params()
        metadata = {
            "pipeline": execution_config.pipeline_id,
            "job details": execution_config.job_details
            | {
                "start_time": start_ts,
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
            },
            "code": execution_config.code_location,
            "job_input_params": input_params | data_access_factory.get_input_params(),
            "execution_stats": {
                "num partitions": num_partitions,
                "execution time, min": (time.time() - start_time) / 60,
            },
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
