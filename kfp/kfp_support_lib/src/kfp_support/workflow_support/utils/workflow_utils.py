import os
import re

import datetime
import time
from typing import Optional, Any

from kfp_server_api import models
from kfp import Client

from kfp_support.api_server_client import KubeRayAPIs
from kfp_support.api_server_client.params import (Template, DEFAULT_HEAD_START_PARAMS, DEFAULT_WORKER_START_PARAMS,
                                                  volume_decoder, HeadNodeSpec, WorkerNodeSpec, ClusterSpec, Cluster,
                                                  RayJobRequest)
from data_processing.utils import ParamsUtils


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
        :param tmout: timeout (sec) (-1 wait forever)
        :param wait: internal wait (sec)
        :return: Completion status and an error message if such exists
        """
        try:
            if tmout > 0:
                end = time.time() + tmout
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
                print(f"Got pipeline execution status {status}")

            if status.lower() in ["succeeded", "completed"]:
                return status, ""
            return status, run_details.run.error

        except Exception as e:
            print(f"Failed waiting pipeline completion {e}")
            return "failed", e.__cause__


class RayRemoteJobs:
    """
    class supporting Ray remote jobs
    """
    def __init__(self, server_url: str = "http://localhost:8080", wait_interval: int = 2):
        """
        Initialization
        :param server_url: API server URL
        :param wait_interval: wai interval
        """
        self.api_server_client = KubeRayAPIs(server_url=server_url, wait_interval=wait_interval)

    def create_ray_cluster(self, name: str, namespace: str, head_node: dict[str, Any],
                           worker_nodes: list[dict[str, Any]]) -> tuple[int, str]:
        """
        Create Ray cluster
        :param name: name
        :param namespace: namespace
        :param head_node: head node specification dictionary including the following:
            mandatory fields:
                cpu - number of cpus
                memory memory size (GB)
                image - image to use
                image_pull_secret - image pull secret
            optional fields:
                gpu - number of gpus
                gpu_accelerator - gpu accelerator to use
                ray_start_params - dictionary of ray start parameters
                volumes - list of volumes for head node
                service_account - service account to use (has to be created)
                environment - dictionary of head node environment
                annotations: dictionary of head node annotation
                labels: dictionary of head node labels

        :param worker_nodes: an array of worker node specification dictionary including the following:
            mandatory fields:
                cpu - number of cpus
                memory memory size (GB)
                image - image to use
                image_pull_secret - image pull secret
                max_replicas - max replicas for this worker group
            optional fields:
                gpu - number of gpus
                gpu_accelerator - gpu accelerator to use
                replicas - number of replicas to create for this group (default 1)
                min_replicas - min number of replicas for this group (default 0)
                ray_start_params - dictionary of ray start parameters
                volumes - list of volumes for this group
                service_account - service account to use (has to be created)
                environment - dictionary of node of this group environment
                annotations: dictionary of node of this group annotation
                labels: dictionary of node of this group labels
        :return:tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # start with templates
        # head_node
        cpus = head_node.get("cpu", 1)
        memory = head_node.get("memory", 1)
        gpus = head_node.get("gpu", 0)
        accelerator = head_node.get("gpu_accelerator", None)
        head_node_teplate_name = f"{name}_head_template"
        _, _ = self.api_server_client.delete_compute_template(ns="default", name=head_node_teplate_name)
        head_template = Template(name=head_node_teplate_name, namespace=namespace, cpu=cpus, memory=memory,
                                 gpu=gpus, gpu_accelerator=accelerator)
        status, error = self.api_server_client.create_compute_template(head_template)
        if status != 200:
            return status, error
        worker_templates = []
        index = 0
        # For every worker group
        for worker_node in worker_nodes:
            cpus = worker_node.get("cpu", 1)
            memory = worker_node.get("memory", 1)
            gpus = worker_node.get("gpu", 0)
            accelerator = worker_node.get("gpu_accelerator", None)
            worker_node_teplate_name = f"{name}_worker_template_{index}"
            _, _ = self.api_server_client.delete_compute_template(ns="default", name=worker_node_teplate_name)
            worker_template = Template(name=worker_node_teplate_name, namespace=namespace, cpu=cpus, memory=memory,
                                       gpu=gpus, gpu_accelerator=accelerator)
            status, error = self.api_server_client.create_compute_template(worker_template)
            if status != 200:
                return status, error
            worker_templates.append(worker_template)
            index += 1
        # Build head node spec
        image = head_node.get("image", "rayproject/ray:2.9.0-py310")
        image_pull_secret = head_node.get("image_pull_secret", None)
        ray_start_params = head_node.get("ray_start_params", DEFAULT_HEAD_START_PARAMS)
        volumes = head_node.get("volumes", None)
        service_account = head_node.get("service_account", None)
        environment = head_node.get("environment", None)
        annotations = head_node.get("annotations", None)
        labels = head_node.get("labels", None)
        if volumes is not None:
            volumes_array = [volume_decoder(v).to_dict() for v in volumes]
        else:
            volumes_array = None
        head_node_spec = HeadNodeSpec(compute_template=head_node_teplate_name, image=image,
                                      ray_start_params=ray_start_params, volumes=volumes_array,
                                      service_account=service_account, image_pull_secret=image_pull_secret,
                                      environment=environment, annotations=annotations, labels=labels)
        # build worker nodes
        worker_groups = []
        index = 0
        for worker_node in worker_nodes:
            max_replicas = worker_node.get("max_replicas", 1)
            replicas = worker_node.get("replicas", 1)
            min_replicas = worker_node.get("max_replicas", 0)
            image = worker_node.get("image", "rayproject/ray:2.9.0-py310")
            image_pull_secret = worker_node.get("image_pull_secret", None)
            ray_start_params = worker_node.get("ray_start_params", DEFAULT_WORKER_START_PARAMS)
            volumes = worker_node.get("volumes", None)
            service_account = worker_node.get("service_account", None)
            environment = worker_node.get("environment", None)
            annotations = worker_node.get("annotations", None)
            labels = worker_node.get("labels", None)
            if volumes is not None:
                volumes_array = [volume_decoder(v).to_dict() for v in volumes]
            else:
                volumes_array = None
            worker_groups.append(WorkerNodeSpec(group_name=f"worker_group_{index}",
                                                compute_template=worker_templates[index], image=image,
                                                max_replicas=max_replicas, replicas=replicas,
                                                min_replicas=min_replicas, ray_start_params=ray_start_params,
                                                volumes=volumes_array, service_account=service_account,
                                                image_pull_secret=image_pull_secret, environment=environment,
                                                annotations=annotations, labels=labels))
            index += 1
        # Build cluster spec
        cluster_spec = ClusterSpec(head_node=head_node_spec, worker_groups=worker_groups)
        # Build cluster
        cluster = Cluster(name=name, namespace=namespace, user="goofy", version="2.9.0", cluster_spec=cluster_spec)
        status, error = self.api_server_client.create_cluster(cluster)
        if status != 200:
            return status, error
        # Wait for cluster ready
        return self.api_server_client.wait_cluster_ready(name=name, ns=namespace)

    def clean_up_ray_cluster(self, name: str, namespace: str) -> tuple[int, str]:
        """
        Clean up Ray cluster and supporting template
        :param name: cluster name
        :param namespace: cluster namespace
        :return:tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # delete cluster
        status, error = self.api_server_client.delete_cluster(ns=namespace, name=name)
        if status != 200:
            return status, error
        # clean up templates
        status, error, template_array = self.api_server_client.list_compute_templates_namespace(ns=namespace)
        if status != 200:
            return status, error
        for template in template_array:
            if template.name.startswith(name):
                status, error = self.api_server_client.delete_compute_template(ns=namespace, name=template.name)
                if status != 200:
                    return status, error
        return status, error

    def submit_job(self, name: str, namespace: str, request: dict[str, Any], runtime_env: str = None,
                   executor: str = "transformer_launcher.py") -> tuple[str, str, str]:
        """
        Submit job for execution
        :param name: cluster name
        :param namespace: cluster namespace
        :param request: dictionary of the remote job request
        :param runtime_env: runtime environment string
        :param executor: python file to execute
        :return:tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            submission id - submission id
        """
        # Build job request
        job_request = RayJobRequest(entrypoint=ParamsUtils.dict_to_req(d=request, executor=executor))
        if runtime_env is not None:
            job_request.runtime_env = runtime_env
        return self.api_server_client.submit_job(ns=namespace, name=name, job_request=job_request)

    def _get_job_status(self, name: str, namespace: str, submission_id: str) -> tuple[str, str, str]:
        """
        Get job status
        :param name: cluster name
        :param namespace: cluster namespace
        :param submission_id: job submission ID
        :return:tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            status - job status
        """
        # get job invo
        status, error, info = self.api_server_client.get_job_info(ns=namespace, name=name, sid=submission_id)
        if status != 200:
            return status, error, ""
        return status, error, info.status
