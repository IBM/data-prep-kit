import time
from datetime import datetime

import ray
from data_processing.data_access import DataAccessFactory
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    RayUtils,
    TransformOrchestratorConfiguration,
    TransformStatistics,
    TransformTableProcessor,
)
from ray.util import ActorPool
from ray.util.metrics import Gauge


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def orchestrate(
    preprocessing_params: TransformOrchestratorConfiguration,
    data_access_factory: DataAccessFactory,
    transform_runtime_config: DefaultTableTransformConfiguration,
) -> int:
    """
    orchestrator for transformer execution
    :param preprocessing_params: orchestrator configuration
    :param data_access_factory: data access factory
    :param transform_runtime_config: transformer runtime configuration
    :return: 0 - success or 1 - failure
    """
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        # create data access
        data_access = data_access_factory.create_data_access()
        if data_access is None:
            print("No DataAccess instance provided - exiting", flush=True)
            return 1
        # Get files to process
        files, profile = data_access.get_files_to_process()
        if len(files) == 0:
            print("No input files to process - exiting", flush=True)
            return 0
        print(f"Number of files is {len(files)}, source profile {profile}", flush=True)
        # Print interval
        print_interval = int(len(files) / 100)
        if print_interval == 0:
            print_interval = 1
        # Get Resources for execution
        resources = RayUtils.get_cluster_resources()
        print(f"Cluster resources: {resources}")
        # print execution params
        print(
            f"Number of workers - {preprocessing_params.n_workers} " f"with {preprocessing_params.worker_options} each"
        )
        # create transformer runtime
        runtime = transform_runtime_config.create_transform_runtime()
        # create statistics
        statistics = TransformStatistics.remote({})
        # create executors
        processor_params = {
            "data_access_factory": data_access_factory,
            "transform_class": transform_runtime_config.get_transform_class(),
            "transform_params": runtime.set_environment(data_access=data_access, statistics=statistics, files=files),
            "statistics": statistics,
        }
        processors = RayUtils.create_actors(
            clazz=TransformTableProcessor,
            params=processor_params,
            actor_options=preprocessing_params.worker_options,
            n_actors=preprocessing_params.n_workers,
            creation_delay=preprocessing_params.creation_delay,
        )
        processors_pool = ActorPool(processors)
        # create gauges
        files_in_progress_gauge = Gauge("files_in_progress", "Number of files in progress")
        files_completed_gauge = Gauge("files_processed_total", "Number of files completed")
        available_cpus_gauge = Gauge("AVAILABLE_CPUS", "Number of available CPUs")
        available_gpus_gauge = Gauge("available_gpus", "Number of available GPUs")
        available_memory_gauge = Gauge("available_memory", "Available memory")
        available_object_memory_gauge = Gauge("available_object_store", "Available object store")
        # process data
        RayUtils.process_files(
            executors=processors_pool,
            files=files,
            print_interval=print_interval,
            files_in_progress_gauge=files_in_progress_gauge,
            files_completed_gauge=files_completed_gauge,
            available_cpus_gauge=available_cpus_gauge,
            available_gpus_gauge=available_gpus_gauge,
            available_memory_gauge=available_memory_gauge,
            object_memory_gauge=available_object_memory_gauge,
        )
        # invoke flush to ensure that all results are returned
        start = time.time()
        replies = [processor.flush.remote() for processor in processors]
        RayUtils.wait_for_execution_completion(replies)
        print(f"done flushing in {time.time() - start} sec")
        # Compute execution statistics
        stats = runtime.compute_execution_stats(ray.get(statistics.get_execution_stats.remote()))

        # build and save metadata
        metadata = {
            "pipeline": preprocessing_params.pipeline_id,
            "job details": preprocessing_params.job_details,
            "code": preprocessing_params.code_location,
            "job_input_params": transform_runtime_config.get_input_params()
            | data_access_factory.get_input_params()
            | preprocessing_params.get_input_params(),
            "execution_stats": resources
            | {"start_time": start_ts, "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            "job_output_stats": stats,
        }
        data_access.save_job_metadata(metadata)
        return 0
    except Exception as e:
        print(f"Exception during execution {e}")
        return 1
