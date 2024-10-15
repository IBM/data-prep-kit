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

import os
import socket
import time
import traceback
from datetime import datetime

import yaml
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics, AbstractFolderTransform
from data_processing.utils import GB, get_logger
from data_processing_spark.runtime.spark import (
    SparkTransformExecutionConfiguration,
    SparkTransformFileProcessor,
    SparkTransformRuntimeConfiguration,
)
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


logger = get_logger(__name__)


def _init_spark(runtime_config: SparkTransformRuntimeConfiguration) -> SparkSession:
    server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
    if server_port_https == -1:
        # running locally
        spark_config = {"spark.driver.host": "127.0.0.1"}
        return SparkSession.builder.appName(runtime_config.get_name()).config(map=spark_config).getOrCreate()
    else:
        # running in Kubernetes, use spark_profile.yml and
        # environment variables for configuration
        server_port = os.environ["KUBERNETES_SERVICE_PORT"]
        master_url = f"k8s://https://kubernetes.default:{server_port}"

        # Read Spark configuration profile
        config_filepath = os.path.abspath(
            os.path.join(os.getenv("SPARK_HOME"), "work-dir", "config", "spark_profile.yml")
        )
        with open(config_filepath, "r") as config_fp:
            spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
        spark_config["spark.submit.deployMode"] = "client"

        # configure the executor pods from template
        executor_pod_template_file = os.path.join(
            os.getenv("SPARK_HOME"),
            "work-dir",
            "src",
            "templates",
            "spark-executor-pod-template.yml",
        )
        spark_config["spark.kubernetes.executor.podTemplateFile"] = executor_pod_template_file
        spark_config["spark.kubernetes.container.image.pullPolicy"] = "Always"

        # Pass the driver IP address to the workers for callback
        myservice_url = socket.gethostbyname(socket.gethostname())
        spark_config["spark.driver.host"] = myservice_url
        spark_config["spark.driver.bindAddress"] = "0.0.0.0"
        spark_config["spark.decommission.enabled"] = True
        logger.info(f"Launching Spark Session with configuration\n" f"{yaml.dump(spark_config, indent=2)}")
        app_name = spark_config.get("spark.app.name", "my-spark-app")
        return SparkSession.builder.master(master_url).appName(app_name).config(map=spark_config).getOrCreate()


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
    bcast_params = runtime_config.get_bcast_params(data_access_factory)
    if data_access is None:
        logger.error("No DataAccess instance provided - exiting")
        return 1
    # initialize Spark
    spark_session = _init_spark(runtime_config)
    sc = spark_session.sparkContext
    # broadcast
    spark_runtime_config = sc.broadcast(runtime_config)
    daf = sc.broadcast(data_access_factory)
    spark_bcast_params = sc.broadcast(bcast_params)

    def process_partition(iterator):
        """
        process partitions
        :param iterator: iterator of records
        :return:
        """
        # local statistics dictionary
        statistics = TransformStatistics()
        # create transformer runtime
        bcast_params = spark_bcast_params.value
        d_access_factory = daf.value
        runtime_conf = spark_runtime_config.value
        runtime = runtime_conf.create_transform_runtime()
        # create file processor
        file_processor = SparkTransformFileProcessor(
            data_access_factory=d_access_factory,
            runtime_configuration=runtime_conf,
            statistics=statistics,
            is_folder=is_folder,
        )
        first = True
        for f in iterator:
            # for every file
            if first:
                logger.debug(f"partition {f}")
                # add additional parameters
                transform_params = (
                    runtime.get_transform_config(
                        partition=int(f[1]), data_access_factory=d_access_factory, statistics=statistics
                    )
                    | bcast_params
                )
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
    is_folder = issubclass(runtime_config.get_transform_class(), AbstractFolderTransform)
    try:
        if is_folder:
            # folder transform
            runtime = runtime_config.create_transform_runtime()
            files = runtime.get_folders(data_access=data_access)
            logger.info(f"Number of folders is {len(files)}")        # Get files to process
        else:
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
        resources = {"cpus": cpus, "gpus": 0, "memory": round(memory / GB, 2), "object_store": 0}
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
            }
            | resources,
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
