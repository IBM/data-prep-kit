from datetime import datetime

import ray

from data_processing.data_access import DataAccessFactory
from data_processing.ray.launcher import AbstractDataTransformRuntimeFactory
from data_processing.ray.transform_statistics import Statistics
from ray_orchestrator_configuration import *
from ray.util import ActorPool
from ray.util.metrics import Gauge
from table_processor import TableProcessor
from ray_utils import RayUtils


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def transform_orchestrator(
    preprocessing_params: RayOrchestratorConfiguration,
    data_access_factory: DataAccessFactory,
    transformer_runtime_factory: AbstractDataTransformRuntimeFactory,
) -> int:
    """
    orchestrator for transformer execution
    :param preprocessing_params: orchestrator configuration
    :param data_access_factory: data access factory
    :param transformer_runtime_factory: transformer runtime factory
    :return: 0 - success or 1 - failure
    """
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        # create data access
        data_access = data_access_factory.create_data_access()
        if data_access is None:
            return 1
        # Get files to process
        files, profile = data_access.get_files_to_process()
        print(f"Number of s3 files is {len(files)}, source profile {profile}")
        if len(files) == 0:
            print("No input files to process - exiting")
            return 0
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
        runtime = transformer_runtime_factory.create_transformer_runtime()
        # create statistics
        statistics = Statistics.remote()
        # create executors
        processor_params = {
            "data_access_factory": data_access_factory,
            "processor": transformer_runtime_factory.get_transformer(),
            "processor_params": runtime.set_environment(data_access=data_access),
            "stats": statistics,
        }
        processors = ActorPool(
            RayUtils.create_actors(
                clazz=TableProcessor,
                params=processor_params,
                actor_options=preprocessing_params.worker_options,
                n_actors=preprocessing_params.n_workers,
                creation_delay=preprocessing_params.creation_delay,
            )
        )
        # create gauges
        files_in_progress_gauge = Gauge("files_in_progress", "Number of files in progress")
        files_completed_gauge = Gauge("files_processed_total", "Number of files completed")
        available_cpus_gauge = Gauge("AVAILABLE_CPUS", "Number of available CPUs")
        available_gpus_gauge = Gauge("available_gpus", "Number of available GPUs")
        available_memory_gauge = Gauge("available_memory", "Available memory")
        available_object_memory_gauge = Gauge("available_object_store", "Available object store")
        # process data
        RayUtils.process_files(
            executors=processors,
            files=files,
            print_interval=print_interval,
            files_in_progress_gauge=files_in_progress_gauge,
            files_completed_gauge=files_completed_gauge,
            available_cpus_gauge=available_cpus_gauge,
            available_gpus_gauge=available_gpus_gauge,
            available_memory_gauge=available_memory_gauge,
            object_memory_gauge=available_object_memory_gauge,
        )
        # Compute execution statistics
        stats = runtime.compute_execution_stats(ray.get(statistics.execution_stats.remote()))

        # build and save metadata
        metadata = {
            "pipeline": preprocessing_params.pipeline_id,
            "job details": preprocessing_params.job_details,
            "code": preprocessing_params.code_location,
            "job_input_params": transformer_runtime_factory.get_input_params_metadata(),
            "execution_stats": resources
            | {"start_time": start_ts, "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            "job_output_stats": stats,
        }
        data_access.save_job_metadata(metadata)
        return 0
    except Exception as e:
        print(f"Exception during execution {e}")
        return 1
