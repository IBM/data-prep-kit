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
from multiprocessing import Pool
import traceback
from datetime import datetime

from data_processing.data_access import DataAccessFactoryBase
from data_processing.runtime.pure_python import (
    PythonTransformExecutionConfiguration,
    PythonTransformRuntimeConfiguration,
    PythonTransformFileProcessor,
    PythonPoolTransformFileProcessor,
)
from data_processing.transform import TransformStatistics
from data_processing.utils import get_logger


logger = get_logger(__name__)


def orchestrate(
    data_access_factory: DataAccessFactoryBase,
    runtime_config: PythonTransformRuntimeConfiguration,
    execution_config: PythonTransformExecutionConfiguration,
) -> int:
    """
    orchestrator for transformer execution
    :param data_access_factory: data access factory
    :param runtime_config: transformer configuration
    :param execution_config: execution configuration
    :return: 0 - success or 1 - failure
    """
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"orchestrator {runtime_config.get_name()} started at {start_ts}")
    # create statistics
    statistics = TransformStatistics()
    # create data access
    data_access = data_access_factory.create_data_access()
    if data_access is None:
        logger.error("No DataAccess instance provided - exiting")
        return 1
    try:
        # Get files to process
        files, profile, retries = data_access.get_files_to_process()
        if len(files) == 0:
            logger.error("No input files to process - exiting")
            return 0
        if retries > 0:
            statistics.add_stats({"data access retries": retries})
        logger.info(f"Number of files is {len(files)}, source profile {profile}")
        # Print interval
        print_interval = int(len(files) / 100)
        if print_interval == 0:
            print_interval = 1
        if execution_config.multiprocessing:
            # using multiprocessor pool for execution
            statistics = _process_transforms_multiprocessor(files=files, size=execution_config.num_processors,
                                                            data_access_factory=data_access_factory,
                                                            print_interval=print_interval,
                                                            runtime_configuration=runtime_config)
        else:
            # using sequential execution
            _process_transforms(files=files, data_access_factory=data_access_factory, print_interval=print_interval,
                                statistics=statistics, runtime_configuration=runtime_config)
        status = "success"
        return_code = 0
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return_code = 1
        status = "failure"
    try:
        # Compute execution statistics
        logger.debug("Computing execution stats")
        stats = statistics.get_execution_stats()
        # build and save metadata
        logger.debug("Building job metadata")
        input_params = runtime_config.get_transform_metadata()
        metadata = {
            "pipeline": execution_config.pipeline_id,
            "job details": execution_config.job_details
            | {
                "start_time": start_ts,
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
            },
            "code": execution_config.code_location,
            "job_input_params":
                input_params | data_access_factory.get_input_params() | execution_config.get_input_params(),
            "job_output_stats": stats,
        }
        logger.debug(f"Saving job metadata: {metadata}.")
        data_access.save_job_metadata(metadata)
        logger.debug("Saved job metadata.")
        return return_code
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return 1


def _process_transforms(files: list[str], print_interval: int, data_access_factory: DataAccessFactoryBase,
                        statistics: TransformStatistics, runtime_configuration: PythonTransformRuntimeConfiguration) \
        -> None:
    """
    Process transforms sequentially
    :param files: list of files to process
    :param statistics: statistics class
    :param print_interval: print interval
    :param data_access_factory: data access factory
    :param runtime_configuration: transform configuration
    :return: metadata for the execution

    :return: None
    """
    # create executor
    executor = PythonTransformFileProcessor(
        data_access_factory=data_access_factory, statistics=statistics, runtime_configuration=runtime_configuration
    )
    # process data
    logger.debug(f"{runtime_configuration.get_name()} Begin processing files")
    t_start = time.time()
    completed = 0
    for path in files:
        executor.process_file(path)
        completed += 1
        if completed % print_interval == 0:
            logger.info(
                f"Completed {completed} files ({100 * completed / len(files)}%) "
                f"in {(time.time() - t_start)/60} min"
            )
    logger.info(f"Done processing {completed} files, waiting for flush() completion.")
    # invoke flush to ensure that all results are returned
    start = time.time()
    executor.flush()
    logger.info(f"done flushing in {time.time() - start} sec")


def _process_transforms_multiprocessor(files: list[str], size: int, print_interval: int,
                                       data_access_factory: DataAccessFactoryBase,
                                       runtime_configuration: PythonTransformRuntimeConfiguration) \
        -> TransformStatistics:
    """
    Process transforms using multiprocessing pool
    :param files: list of files to process
    :param size: pool size
    :param print_interval: print interval
    :param data_access_factory: data access factory
    :param runtime_configuration: transform configuration
    :return: metadata for the execution
    """
    # result statistics
    statistics = TransformStatistics()
    # create processor
    processor = PythonPoolTransformFileProcessor(data_access_factory=data_access_factory,
                                                 runtime_configuration=runtime_configuration)
    completed = 0
    t_start = time.time()
    # create multiprocessing pool
    with Pool(processes=size) as pool:
        # execute for every input file
        for result in pool.imap_unordered(processor.process_file, files):
            completed += 1
            # accumulate statistics
            statistics.add_stats(result)
            if completed % print_interval == 0:
                # print intermediate statistics
                logger.info(
                    f"Completed {completed} files ({100 * completed / len(files)}%) "
                    f"in {(time.time() - t_start)/60} min"
                )
        logger.info(f"Done processing {completed} files, waiting for flush() completion.")
        results = [{}] * size
        # flush
        for i in range(size):
            results[i] = pool.apply_async(processor.flush)
        for s in results:
            statistics.add_stats(s.get())
    logger.info(f"done flushing in {time.time() - t_start} sec")
    return statistics
