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
from data_processing.pure_python import TransformTableProcessor
from data_processing.transform import (
    TransformExecutionConfiguration,
    TransformStatistics,
)
from data_processing.pure_python import PythonLauncherConfiguration
from data_processing.utils import get_logger


logger = get_logger(__name__)


def orchestrate(
    data_access_factory: DataAccessFactoryBase,
    transform_config: PythonLauncherConfiguration,
    execution_config: TransformExecutionConfiguration,
) -> int:
    """
    orchestrator for transformer execution
    :param data_access_factory: data access factory
    :param transform_config: transformer configuration
    :return: 0 - success or 1 - failure
    """
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"orchestrator {transform_config.get_name()} started at {start_ts}")
    try:
        # create data access
        data_access = data_access_factory.create_data_access()
        if data_access is None:
            logger.error("No DataAccess instance provided - exiting")
            return 1
        # Get files to process
        files, profile = data_access.get_files_to_process()
        if len(files) == 0:
            logger.error("No input files to process - exiting")
            return 0
        logger.info(f"Number of files is {len(files)}, source profile {profile}")
        # Print interval
        print_interval = int(len(files) / 100)
        if print_interval == 0:
            print_interval = 1
        # create statistics
        statistics = TransformStatistics()
        # create executor
        executor = TransformTableProcessor(
            data_access_factory=data_access_factory, statistics=statistics, params=transform_config
        )
        # process data
        logger.debug(f"{transform_config.get_name()} Begin processing files")
        t_start = time.time()
        completed = 0
        for path in files:
            executor.process_data(path)
            completed += 1
            if completed % print_interval == 0:
                logger.info(f"Completed {completed} files in {(time.time() - t_start)/60} min")
        logger.debug("Done processing files, waiting for flush() completion.")
        # invoke flush to ensure that all results are returned
        start = time.time()
        executor.flush()
        logger.info(f"done flushing in {time.time() - start} sec")
        # Compute execution statistics
        logger.debug("Computing execution stats")
        stats = statistics.get_execution_stats()
        # build and save metadata
        logger.debug("Building job metadata")
        input_params = transform_config.get_transform_metadata()
        metadata = {
            "pipeline": execution_config.pipeline_id,
            "job details": execution_config.job_details
            | {
                "start_time": start_ts,
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "success",
            },
            "code": execution_config.code_location,
            "job_input_params": input_params | data_access_factory.get_input_params(),
            "job_output_stats": stats,
        }
        logger.debug(f"Saved job metadata: {metadata}.")
        data_access.save_job_metadata(metadata)
        logger.debug("Saved job metadata.")
        return 0
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return 1
