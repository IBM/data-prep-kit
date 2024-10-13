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

import ray
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import AbstractFolderTransform
from data_processing_ray.runtime.ray import (
    RayTransformExecutionConfiguration,
    RayTransformFileProcessor,
    RayTransformRuntimeConfiguration,
    RayUtils,
    TransformStatisticsRay,
)
from ray.util import ActorPool


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def orchestrate(
    preprocessing_params: RayTransformExecutionConfiguration,
    data_access_factory: DataAccessFactoryBase,
    runtime_config: RayTransformRuntimeConfiguration,
) -> int:
    """
    orchestrator for transformer execution
    :param preprocessing_params: orchestrator configuration
    :param data_access_factory: data access factory
    :param runtime_config: transformer runtime configuration
    :return: 0 - success or 1 - failure
    """

    from data_processing.utils import get_logger
    from ray.util.metrics import Gauge

    logger = get_logger(__name__)
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    logger.info(f"orchestrator started at {start_ts}")
    # create data access
    data_access = data_access_factory.create_data_access()
    if data_access is None:
        logger.error("No DataAccess instance provided - exiting")
        return 1
    statistics = TransformStatisticsRay.remote({})
    # create transformer runtime
    runtime = runtime_config.create_transform_runtime()
    resources = RayUtils.get_cluster_resources()
    is_folder = issubclass(runtime_config.get_transform_class(), AbstractFolderTransform)
    try:
        if is_folder:
            # folder transform
            files = runtime.get_folders(data_access=data_access)
            logger.info(f"Number of folders is {len(files)}")        # Get files to process
        else:
            files, profile, retries = data_access.get_files_to_process()
            if len(files) == 0:
                logger.error("No input files to process - exiting")
                return 0
            # log retries
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            logger.info(f"Number of files is {len(files)}, source profile {profile}")
        # Print interval
        print_interval = int(len(files) / 100)
        if print_interval == 0:
            print_interval = 1
        # Get Resources for execution
        logger.info(f"Cluster resources: {resources}")
        # print execution params
        logger.info(
            f"Number of workers - {preprocessing_params.n_workers} " f"with {preprocessing_params.worker_options} each"
        )
        # create executors
        processor_params = {
            "data_access_factory": data_access_factory,
            "transform_class": runtime_config.get_transform_class(),
            "transform_params": runtime.get_transform_config(
                data_access_factory=data_access_factory, statistics=statistics, files=files
            ),
            "statistics": statistics,
            "is_folder": is_folder,
        }
        logger.debug("Creating actors")
        processors = RayUtils.create_actors(
            clazz=RayTransformFileProcessor,
            params=processor_params,
            actor_options=preprocessing_params.worker_options,
            n_actors=preprocessing_params.n_workers,
            creation_delay=preprocessing_params.creation_delay,
        )
        processors_pool = ActorPool(processors)
        # create gauges
        files_in_progress_gauge = Gauge("files_in_progress", "Number of files in progress")
        files_completed_gauge = Gauge("files_processed_total", "Number of files completed")
        available_cpus_gauge = Gauge("available_cpus", "Number of available CPUs")
        available_gpus_gauge = Gauge("available_gpus", "Number of available GPUs")
        available_memory_gauge = Gauge("available_memory", "Available memory")
        available_object_memory_gauge = Gauge("available_object_store", "Available object store")
        # process data
        logger.debug("Begin processing files")
        failures = RayUtils.process_files(
            executors=processors_pool,
            files=files,
            print_interval=print_interval,
            files_in_progress_gauge=files_in_progress_gauge,
            files_completed_gauge=files_completed_gauge,
            available_cpus_gauge=available_cpus_gauge,
            available_gpus_gauge=available_gpus_gauge,
            available_memory_gauge=available_memory_gauge,
            object_memory_gauge=available_object_memory_gauge,
            logger=logger,
        )
        if failures > 0:
            statistics.add_stats.remote({"actor failures": failures})
        logger.debug("Done processing files, waiting for flush() completion.")
        # invoke flush to ensure that all results are returned
        start = time.time()
        replies = [processor.flush.remote() for processor in processors]
        failures = RayUtils.wait_for_execution_completion(logger=logger, replies=replies)
        if failures > 0:
            statistics.add_stats.remote({"actor failures": failures})
        logger.info(f"done flushing in {round(time.time() - start, 3)} sec")
        status = "success"
        return_code = 0
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        status = "failure"
        return_code = 1
    try:
        # Compute execution statistics
        logger.debug("Computing execution stats")
        stats = runtime.compute_execution_stats(ray.get(statistics.get_execution_stats.remote()))
        stats["processing_time"] = round(stats["processing_time"], 3)

        # build and save metadata
        logger.debug("Building job metadata")
        metadata = {
            "pipeline": preprocessing_params.pipeline_id,
            "job details": preprocessing_params.job_details
            | {"start_time": start_ts, "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "status": status},
            "code": preprocessing_params.code_location,
            "job_input_params": runtime_config.get_transform_metadata()
            | data_access_factory.get_input_params()
            | preprocessing_params.get_input_params(),
            "execution_stats": resources | {"execution time, min": round((time.time() - start_time) / 60.0, 3)},
            "job_output_stats": stats,
        }
        logger.debug(f"Saving job metadata: {metadata}.")
        data_access.save_job_metadata(metadata)
        logger.debug("Saved job metadata.")
        return return_code
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return 1
