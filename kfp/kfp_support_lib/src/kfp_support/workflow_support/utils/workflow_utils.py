import os
import re

import datetime
import time
from typing import Optional, Any

from kfp_server_api import models
from kfp import Client


ONE_HOUR_SEC = 60 * 60
ONE_DAY_SEC = ONE_HOUR_SEC * 24
ONE_WEEK_SEC = ONE_DAY_SEC * 7


class KFPUtils:
    """
    Helper utilities for KFP implementations
    """
    @staticmethod
    def credentials(access_key: str = "COS_KEY", secret_key: str = "COS_SECRET", endpoint: str = "COS_ENDPOINT") \
            -> tuple[str, str, str]:
        """
        Get credentials from the environment
        :param access_key: environment variable for access key
        :param secret_key: environment variable for secret key
        :param endpoint: environment variable for S3 endpoint
        :return:
        """
        cos_key = os.getenv(access_key, "")
        cos_secret = os.getenv(secret_key, "")
        cos_endpoint = os.getenv(endpoint, "")
        return cos_key, cos_secret, cos_endpoint

    @staticmethod
    def get_namespace() -> str:
        """
        Get k8 namespace that we are running it
        :return:
        """
        ns = ""
        try:
            file = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r")
        except Exception as e:
            print(f"Failed to open /var/run/secrets/kubernetes.io/serviceaccount/namespace file, exception {e}")
        else:
            with file:
                ns = file.read()
        return ns

    @staticmethod
    def clean_path(path: str = "") -> str:
        """
        Clean path parameters:
            Removes white spaces from the input/output paths
            Removes schema prefix (s3://, http:// https://), if exists
            Adds the "/" character at the end, if it doesn't exist
            Removed URL encoding
        :param path: path to clean up
        :return: clean path
        """
        path = path.strip()
        if path == "":
            return path
        from urllib.parse import unquote, urlparse, urlunparse

        # Parse the URL
        parsed_url = urlparse(path)
        if parsed_url.scheme in ["http", "https"]:
            # Remove host
            parsed_url = parsed_url._replace(netloc="")
            parsed_url = parsed_url._replace(path=parsed_url.path[1:])

        # Remove the schema
        parsed_url = parsed_url._replace(scheme="")

        # Reconstruct the URL without the schema
        url_without_schema = urlunparse(parsed_url)

        # Remove //
        if url_without_schema[:2] == "//":
            url_without_schema = url_without_schema.replace("//", "", 1)

        return_path = unquote(url_without_schema)
        if return_path[-1] != "/":
            return_path += "/"
        return return_path

    @staticmethod
    def runtime_name(ray_name: str = "", run_id: str = "") -> str:
        """
        Get unique runtime name
        :param ray_name:
        :param run_id:
        :return: runtime name
        """
        # K8s objects cannot contain special characters, except '_', All characters should be in lower case.
        if ray_name != "":
            ray_name = ray_name.replace("_", "-").lower()
            pattern = r"[^a-zA-Z0-9-]"  # the ray_name cannot contain upper case here, but leave it just in case.
            ray_name = re.sub(pattern, "", ray_name)
        else:
            ray_name = "a"
        # the return value plus namespace name will be the name of the Ray Route,
        # which length is restricted to 64 characters,
        # therefore we restrict the return name by 15 character.
        if run_id != "":
            return f"{ray_name[:9]}-{run_id[:5]}"
        return ray_name[:15]


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
            run_id = self.kfp_client.run_pipeline(experiment_id=experiment.id, job_name=job_name,
                                                  pipeline_id=pipeline.id, params=params)
            print("Pipeline submitted")
            return run_id.id
        except Exception as e:
            print(f"Exception starting pipeline {e}")
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
            print(f"Exception getting experiment {e}")
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
            pipelines = self.kfp_client.list_pipelines(page_size=np).pipelines
            required = list(filter(lambda p: name in p.name, pipelines))
            if len(required) != 1:
                print(f"Failure to get pipeline. Number of pipelines with name {name} is {len(required)}")
                return None
            return required[0]

        except Exception as e:
            print(f"Exception getting pipeline {e}")
            return None

    def wait_pipeline_completion(self, run_id: str, tmout: int = -1, wait: int = 600) -> tuple[str, str]:
        """
        Waits for a pipeline run to complete
        :param run_id: run id
        :param tmout: timeout (-1 wait forever)
        :param wait: internal wait (sec)
        :return: Completion status and an error message if such exists
        """
        try:
            if tmout > 0:
                end = time.time() + tmout
            else:
                end = 2**63 - 1
            run_state = self.kfp_client.get_run(run_id=run_id)
            status = run_state.run.status
            while status is None or status.lower() not in ["succeeded", "completed", "failed", "skipped", "error"]:
                time.sleep(60 * wait)
                if (end - time.time()) < 0:
                    return "failed", f"Execution is taking too long"
                run_state = self.kfp_client.get_run(run_id=run_id)
                status = run_state.run.status
                print(f"Got pipeline execution status {status}")

            if status.lower() in ["succeeded", "completed"]:
                return status, ""
            return status, run_state.run.error

        except Exception as e:
            print(f"Failed waiting pipeline completion {e}")
            return "failed", e.__cause__
