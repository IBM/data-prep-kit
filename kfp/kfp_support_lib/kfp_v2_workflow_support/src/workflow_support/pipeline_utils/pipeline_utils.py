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
import json
from typing import Any, Optional

import kfp_server_api
from data_processing.utils import get_logger

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
        description: str = None,
    ) -> kfp_server_api.V2beta1Pipeline:
        """
        Uploads the pipeline. If the pipeline exists then a new version is uploaded
        :param pipeline_package_path: Local path to the pipeline package.
        :param pipeline_name: Optional. Name of the pipeline to be shown in the UI
        :param description: Optional. Description of the pipeline to be shown in the UI.
        :return: Server response object containing pipeline id and other information.
        """
        pipeline_version_name = pipeline_name + " " + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        pipeline = self.get_pipeline_by_name(name=pipeline_name)
        if pipeline is not None:
            try:
                logger.info(f"Upload a new pipeline version")
                self.kfp_client.upload_pipeline_version(
                    pipeline_version_name=pipeline_version_name,
                    pipeline_package_path=pipeline_package_path,
                    pipeline_id=pipeline.pipeline_id,
                )
                return pipeline
            except Exception as e:
                logger.warning(f"Exception upload pipeline {e}")
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

    def start_pipeline(
        self,
        pipeline: kfp_server_api.V2beta1Pipeline,
        experiment: kfp_server_api.V2beta1Experiment,
        params: Optional[dict[str, Any]],
        pipeline_version_id: str = None,
    ) -> str:
        """
        Start a specified pipeline.
        :param pipeline: pipeline definition
        :param experiment: experiment to use
        :param params: pipeline parameters
        :return: the id of the run object
        """
        job_name = pipeline.display_name + " " + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        if pipeline_version_id is None:
            try:
                logger.info(f"pipeline_version_id was not provided. Running latest pipeline version")
                versions = self.kfp_client.list_pipeline_versions(
                    pipeline_id=pipeline.pipeline_id, sort_by="created_at desc"
                )
                latest_version = versions.pipeline_versions[0]
                logger.info(f"running version name {latest_version.display_name}")
                pipeline_version_id = latest_version.pipeline_version_id
            except Exception as e:
                logger.warning(f"Exception list pipelines {e}")
                return None
        try:
            logger.warning(f"Running pipeline version {pipeline_version_id}")
            run_id = self.kfp_client.run_pipeline(
                version_id=pipeline_version_id,
                experiment_id=experiment.experiment_id,
                job_name=job_name,
                pipeline_id=pipeline.pipeline_id,
                params=params,
            )
            logger.info(f"Pipeline run {job_name} submitted")
            return run_id.run_id
        except Exception as e:
            logger.warning(f"Exception starting pipeline {e}")
            return None

    def get_experiment_by_name(self, name: str = "Default") -> kfp_server_api.V2beta1Experiment:
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

    def get_pipeline_by_name(self, name: str, np: int = 100) -> kfp_server_api.V2beta1Pipeline:
        """
        Given pipeline name, return the pipeline
        :param name: pipeline name
        :param np: page size for pipeline query. For large clusters with many pipelines, you might need to
                   increase this number
        :return: pipeline
        """
        try:
            pipeline_filter = json.dumps(
                {
                    "predicates": [
                        {
                            "operation": 1,
                            "key": "display_name",
                            "stringValue": name,
                        }
                    ]
                }
            )
            result = self.kfp_client.list_pipelines(filter=pipeline_filter, page_size=np, sort_by="created_at desc")
            if result.pipelines is None:
                return None
            if len(result.pipelines) == 1:
                return result.pipelines[0]
            elif len(result.pipelines) > 1:
                raise ValueError(f"Multiple pipelines with the name: {name} found, the name needs to be unique.")
        except Exception as e:
            logger.warning(f"Exception getting pipeline {e}")
            return None

    def wait_pipeline_completion(self, run_id: str, timeout: int = None, wait: int = 5) -> str:
        """
        Waits for a pipeline run to complete
        :param run_id: run id
        :param timeout: timeout (sec) (-1 wait forever)
        :param wait: internal wait (sec)
        :return: error message if such exists
        """
        timeout = timeout or datetime.timedelta.max
        try:
            run_response = self.kfp_client.wait_for_run_completion(run_id, timeout=timeout, sleep_duration=wait)
            state = run_response.state.lower()
            if state != "succeeded":
                # Execution failed
                logger.warning(f"Pipeline failed with status {state}")
                return f"Run Failed with status {state}"
            return None
        except Exception as e:
            logger.warning(f"Failed waiting pipeline completion {e}")
            return str(e)
