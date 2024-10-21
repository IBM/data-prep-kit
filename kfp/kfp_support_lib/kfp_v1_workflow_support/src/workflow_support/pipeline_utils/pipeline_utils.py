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

import datetime
import time
from typing import Any, Optional

from data_processing.utils import get_logger
from kfp_server_api import models

from kfp import Client


logger = get_logger(__name__)


class PipelinesUtils:
    """
    Helper class for pipeline management
    """

    def __init__(self, host: str = "http://localhost:8080"):
        """
        Initialization
        :param host: host to connect to
        """
        self.kfp_client = Client(host=host)

    def upload_pipeline(
        self,
        pipeline_package_path: str = None,
        pipeline_name: str = None,
        overwrite: bool = False,
        description: str = None,
    ) -> models.api_pipeline.ApiPipeline:
        """
        Uploads the pipeline
        :param pipeline_package_path: Local path to the pipeline package.
        :param pipeline_name: Optional. Name of the pipeline to be shown in the UI
        :param overwrite: Optional. If pipeline exists, delete it before creating a new one.
        :param description: Optional. Description of the pipeline to be shown in the UI.
        :return: Server response object containing pipeline id and other information.
        """
        if overwrite:
            pipeline = self.get_pipeline_by_name(name=pipeline_name)
            if pipeline is not None:
                try:
                    logger.info(f"pipeline {pipeline_name} already exists. Trying to delete it.")
                    self.kfp_client.delete_pipeline(pipeline_id=pipeline.id)
                except Exception as e:
                    logger.warning(f"Exception deleting pipeline {e} before uploading")
                    return None
        try:
            pipeline = self.kfp_client.upload_pipeline(
                pipeline_package_path=pipeline_package_path, pipeline_name=pipeline_name, description=description
            )
        except Exception as e:
            logger.warning(f"Exception uploading pipeline {e}")
            return None
        if pipeline is None:
            logger.warning(f"Failed to upload pipeline {pipeline_name}.")
            return None
        logger.info("Pipeline uploaded")
        return pipeline

    def delete_pipeline(self, pipeline_id):
        """
        Delete pipeline.
        :param pipeline_id: id of the pipeline.
        :return
        Returns:
          Object. If the method is called asynchronously, returns the request thread.
        Raises:
          kfp_server_api.ApiException: If pipeline is not found.
        """
        return self.kfp_client.delete_pipeline(pipeline_id)

    def start_pipeline(
        self,
        pipeline: models.api_pipeline.ApiPipeline,
        experiment: models.api_experiment.ApiExperiment,
        params: Optional[dict[str, Any]],
    ) -> str:
        """
        Start a specified pipeline.
        :param pipeline: pipeline definition
        :param experiment: experiment to use
        :param params: pipeline parameters
        :return: the id of the run object
        """
        job_name = pipeline.name + " " + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        try:
            run_id = self.kfp_client.run_pipeline(
                experiment_id=experiment.id, job_name=job_name, pipeline_id=pipeline.id, params=params
            )
            logger.info(f"Pipeline run {job_name} submitted")
            return run_id.id
        except Exception as e:
            logger.warning(f"Exception starting pipeline {e}")
            return None

    def get_experiment_by_name(self, name: str = "Default") -> models.api_experiment.ApiExperiment:
        """
        Get experiment by name
        :param name: name
        :return: experiment
        """
        try:
            return self.kfp_client.get_experiment(experiment_name=name)
        except Exception as e:
            logger.warning(f"Exception getting experiment {e}")
            return None

    def get_pipeline_by_name(self, name: str, np: int = 100) -> models.api_pipeline.ApiPipeline:
        """
        Given pipeline name, return the pipeline
        :param name: pipeline name
        :param np: page size for pipeline query. For large clusters with many pipelines, you might need to
                   increase this number
        :return: pipeline
        """
        try:
            # Get all pipelines
            pipelines = self.kfp_client.list_pipelines(page_size=np, sort_by="created_at desc").pipelines
            required = list(filter(lambda p: name in p.name, pipelines))
            if len(required) != 1:
                logger.warning(f"Failure to get pipeline. Number of pipelines with name {name} is {len(required)}")
                return None
            return required[0]

        except Exception as e:
            logger.warning(f"Exception getting pipeline {e}")
            return None

    def wait_pipeline_completion(self, run_id: str, timeout: int = -1, wait: int = 600) -> tuple[str, str]:
        """
        Waits for a pipeline run to complete
        :param run_id: run id
        :param timeout: timeout (sec) (-1 wait forever)
        :param wait: internal wait (sec)
        :return: Completion status and an error message if such exists
        """
        try:
            if timeout > 0:
                end = time.time() + timeout
            else:
                end = 2**63 - 1
            run_details = self.kfp_client.get_run(run_id=run_id)
            status = run_details.run.status
            while status is None or status.lower() not in ["succeeded", "completed", "failed", "skipped", "error"]:
                time.sleep(wait)
                if (end - time.time()) < 0:
                    return "failed", f"Execution is taking too long"
                run_details = self.kfp_client.get_run(run_id=run_id)
                status = run_details.run.status
                logger.info(f"Got pipeline execution status {status}")

            if status.lower() in ["succeeded", "completed"]:
                return status, ""
            return status, run_details.run.error

        except Exception as e:
            logger.warning(f"Failed waiting pipeline completion {e}")
            return "failed", str(e)
