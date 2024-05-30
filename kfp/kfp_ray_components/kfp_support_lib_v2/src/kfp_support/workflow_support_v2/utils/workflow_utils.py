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
import os
import re
import sys
import time
from typing import Any, Optional

from data_processing.data_access import DataAccess
from data_processing.utils import get_logger
import  kfp_server_api
from kfp_support.api_server_client import KubeRayAPIs
from kfp_support.api_server_client.params import (
    DEFAULT_HEAD_START_PARAMS,
    DEFAULT_WORKER_START_PARAMS,
    Cluster,
    ClusterSpec,
    HeadNodeSpec,
    RayJobRequest,
    Template,
    WorkerNodeSpec,
    environment_variables_decoder,
    volume_decoder,
)
from ray.job_submission import JobStatus

logger = get_logger(__name__)

ONE_HOUR_SEC = 60 * 60
ONE_DAY_SEC = ONE_HOUR_SEC * 24
ONE_WEEK_SEC = ONE_DAY_SEC * 7

class KFPUtils:
    """
    Helper utilities for KFP implementations
    """

    @staticmethod
    def credentials(
        access_key: str = "S3_KEY", secret_key: str = "S3_SECRET", endpoint: str = "ENDPOINT"
    ) -> tuple[str, str, str]:
        """
        Get credentials from the environment
        :param access_key: environment variable for access key
        :param secret_key: environment variable for secret key
        :param endpoint: environment variable for S3 endpoint
        :return:
        """
        s3_key = os.getenv(access_key, None)
        s3_secret = os.getenv(secret_key, None)
        s3_endpoint = os.getenv(endpoint, None)
        if s3_key is None or s3_secret is None or s3_endpoint is None:
            logger.warning("Failed to load s3 credentials")
        return s3_key, s3_secret, s3_endpoint

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
            logger.warning(
                f"Failed to open /var/run/secrets/kubernetes.io/serviceaccount/namespace file, " f"exception {e}"
            )
        else:
            with file:
                ns = file.read()
        return ns

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

    @staticmethod
    def dict_to_req(d: dict[str, Any], executor: str = "transformer_launcher.py") -> str:
        res = f"python {executor} "
        for key, value in d.items():
            if isinstance(value, str):
                res += f'--{key}="{value}" '
            else:
                res += f"--{key}={value} "
        return res

    # Load a string that represents a json to python dictionary
    @staticmethod
    def load_from_json(js: str) -> dict[str, Any]:
        try:
            return json.loads(js)
        except Exception as e:
            logger.warning(f"Failed to load parameters {js} with error {e}")
            sys.exit(1)

class RayRemoteJobs:
    """
    class supporting Ray remote jobs
    """

    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

    def __init__(
        self,
        server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
        default_image: str = "rayproject/ray:2.9.3-py310",
        http_retries: int = 5,
        wait_interval: int = 2,
    ):
        """
        Initialization
        :param server_url: API server URL. Default value is assuming running inside the cluster
        :param default_image - default Ray image
        :param wait_interval: wait interval
        :param http_retries: http retries
        """
        self.api_server_client = KubeRayAPIs(
            server_url=server_url, http_retries=http_retries, wait_interval=wait_interval
        )
        self.default_image = default_image

    def create_ray_cluster(
        self,
        name: str,
        namespace: str,
        head_node: dict[str, Any],
        worker_nodes: list[dict[str, Any]],
        wait_cluster_ready: int = -1,
    ) -> tuple[int, str]:
        """
        Create Ray cluster
        :param name: name, _ are not allowed in the name
        :param namespace: namespace
        :param head_node: head node specification dictionary including the following:
            mandatory fields:
                cpu - number of cpus
                memory memory size (GB)
                image - image to use
            optional fields:
                gpu - number of gpus
                gpu_accelerator - gpu accelerator to use
                image_pull_secret - image pull secret
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
                max_replicas - max replicas for this worker group
            optional fields:
                gpu - number of gpus
                gpu_accelerator - gpu accelerator to use
                replicas - number of replicas to create for this group (default 1)
                min_replicas - min number of replicas for this group (default 0)
                image_pull_secret - image pull secret
                ray_start_params - dictionary of ray start parameters
                volumes - list of volumes for this group
                service_account - service account to use (has to be created)
                environment - dictionary of node of this group environment
                annotations: dictionary of node of this group annotation
                labels: dictionary of node of this group labels
        :param wait_cluster_ready - time to wait for cluster ready sec (-1 forever)
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
        head_node_template_name = f"{name}-head-template"
        _, _ = self.api_server_client.delete_compute_template(ns=namespace, name=head_node_template_name)
        head_template = Template(
            name=head_node_template_name,
            namespace=namespace,
            cpu=cpus,
            memory=memory,
            gpu=gpus,
            gpu_accelerator=accelerator,
        )
        status, error = self.api_server_client.create_compute_template(head_template)
        if status != 200:
            return status, error
        worker_template_names = [""] * len(worker_nodes)
        index = 0
        # For every worker group
        for worker_node in worker_nodes:
            cpus = worker_node.get("cpu", 1)
            memory = worker_node.get("memory", 1)
            gpus = worker_node.get("gpu", 0)
            accelerator = worker_node.get("gpu_accelerator", None)
            worker_node_template_name = f"{name}-worker-template-{index}"
            _, _ = self.api_server_client.delete_compute_template(ns=namespace, name=worker_node_template_name)
            worker_template = Template(
                name=worker_node_template_name,
                namespace=namespace,
                cpu=cpus,
                memory=memory,
                gpu=gpus,
                gpu_accelerator=accelerator,
            )
            status, error = self.api_server_client.create_compute_template(worker_template)
            if status != 200:
                return status, error
            worker_template_names[index] = worker_node_template_name
            index += 1
        # Build head node spec
        image = head_node.get("image", self.default_image)
        image_pull_secret = head_node.get("image_pull_secret", None)
        ray_start_params = head_node.get("ray_start_params", DEFAULT_HEAD_START_PARAMS)
        volumes_dict = head_node.get("volumes", None)
        service_account = head_node.get("service_account", None)
        environment_dict = head_node.get("environment", None)
        annotations = head_node.get("annotations", None)
        labels = head_node.get("labels", None)
        if volumes_dict is None:
            volumes = None
        else:
            volumes = [volume_decoder(v) for v in volumes_dict]
        if environment_dict is None:
            environment = None
        else:
            environment = environment_variables_decoder(environment_dict)
        head_node_spec = HeadNodeSpec(
            compute_template=head_node_template_name,
            image=image,
            ray_start_params=ray_start_params,
            volumes=volumes,
            service_account=service_account,
            image_pull_secret=image_pull_secret,
            environment=environment,
            annotations=annotations,
            labels=labels,
        )
        # build worker nodes
        worker_groups = []
        index = 0
        for worker_node in worker_nodes:
            max_replicas = worker_node.get("max_replicas", 1)
            replicas = worker_node.get("replicas", 1)
            min_replicas = worker_node.get("min_replicas", 0)
            image = worker_node.get("image", self.default_image)
            image_pull_secret = worker_node.get("image_pull_secret", None)
            ray_start_params = worker_node.get("ray_start_params", DEFAULT_WORKER_START_PARAMS)
            volumes_dict = worker_node.get("volumes", None)
            service_account = worker_node.get("service_account", None)
            environment_dict = worker_node.get("environment", None)
            annotations = worker_node.get("annotations", None)
            labels = worker_node.get("labels", None)
            if volumes_dict is None:
                volumes = None
            else:
                volumes = [volume_decoder(v) for v in volumes_dict]
            if environment_dict is None:
                environment = None
            else:
                environment = environment_variables_decoder(environment_dict)
            worker_groups.append(
                WorkerNodeSpec(
                    group_name=f"worker-group-{index}",
                    compute_template=worker_template_names[index],
                    image=image,
                    max_replicas=max_replicas,
                    replicas=replicas,
                    min_replicas=min_replicas,
                    ray_start_params=ray_start_params,
                    volumes=volumes,
                    service_account=service_account,
                    image_pull_secret=image_pull_secret,
                    environment=environment,
                    annotations=annotations,
                    labels=labels,
                )
            )
            index += 1
        # Build cluster spec
        cluster_spec = ClusterSpec(head_node=head_node_spec, worker_groups=worker_groups)
        # Build cluster
        cluster = Cluster(name=name, namespace=namespace, user="dataprep", version="2.9.3", cluster_spec=cluster_spec)
        status, error = self.api_server_client.create_cluster(cluster)
        if status != 200:
            return status, error
        # Wait for cluster ready
        return self.api_server_client.wait_cluster_ready(name=name, ns=namespace, wait=wait_cluster_ready)

    def delete_ray_cluster(self, name: str, namespace: str) -> tuple[int, str]:
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

    def submit_job(
        self,
        name: str,
        namespace: str,
        request: dict[str, Any],
        runtime_env: str = None,
        executor: str = "transformer_launcher.py",
    ) -> tuple[int, str, str]:
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
        job_request = RayJobRequest(entrypoint=KFPUtils.dict_to_req(d=request, executor=executor))
        if runtime_env is not None:
            job_request.runtime_env = runtime_env
        return self.api_server_client.submit_job(ns=namespace, name=name, job_request=job_request)

    def _get_job_status(self, name: str, namespace: str, submission_id: str) -> tuple[int, str, str]:
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
        # get job info
        status, error, info = self.api_server_client.get_job_info(ns=namespace, name=name, sid=submission_id)
        if status // 100 != 2:
            return status, error, ""
        return status, error, info.status

    @staticmethod
    def _print_log(log: str, previous_log_len: int) -> None:
        """
        Prints the delta between current and previous logs
        :param log: current log
        :param previous_log_len: previous log length
        :return: None
        """
        l_to_print = log[previous_log_len:]
        if len(l_to_print) > 0:
            l_to_print = RayRemoteJobs.ansi_escape.sub("", l_to_print)
            print(l_to_print)

    def follow_execution(
        self,
        name: str,
        namespace: str,
        submission_id: str,
        data_access: DataAccess = None,
        job_ready_timeout: int = 600,
        print_timeout: int = 120,
    ) -> None:
        """
        Follow remote job execution
        :param name: cluster name
        :param namespace: cluster namespace
        :param submission_id: job submission ID
        :param data_access - data access class
        :param job_ready_timeout: timeout to wait for fob to become ready
        :param print_timeout: print interval
        :return: None
        """
        # Wait for job to start running
        job_status = JobStatus.PENDING
        while job_status != JobStatus.RUNNING and job_ready_timeout > 0:
            status, error, job_status = self._get_job_status(
                name=name, namespace=namespace, submission_id=submission_id
            )
            if status // 100 != 2:
                sys.exit(1)
            if job_status in {JobStatus.STOPPED, JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.RUNNING}:
                break
            time.sleep(self.api_server_client.wait_interval)
            job_ready_timeout -= self.api_server_client.wait_interval
        logger.info(f"job status is {job_status}")
        if job_ready_timeout <= 0:
            logger.warning("timed out waiting for job become ready, exiting")
            sys.exit(1)
        #  While job is running print log
        previous_log_len = 0
        # At this point job could succeeded, failed, stop or running. So print log regardless
        status, error, log = self.api_server_client.get_job_log(ns=namespace, name=name, sid=submission_id)
        if status // 100 != 2:
            sys.exit(1)
        self._print_log(log=log, previous_log_len=previous_log_len)
        previous_log_len = len(log)
        # continue printing log, while job is running
        while job_status == JobStatus.RUNNING:
            time.sleep(print_timeout)
            status, error, log = self.api_server_client.get_job_log(ns=namespace, name=name, sid=submission_id)
            if status // 100 != 2:
                sys.exit(1)
            self._print_log(log=log, previous_log_len=previous_log_len)
            previous_log_len = len(log)
            status, error, job_status = self._get_job_status(
                name=name, namespace=namespace, submission_id=submission_id
            )
            if status // 100 != 2:
                sys.exit(1)
        # Print the final log and execution status
        # Sleep here to avoid racing conditions
        time.sleep(2)
        status, error, log = self.api_server_client.get_job_log(ns=namespace, name=name, sid=submission_id)
        if status // 100 != 2:
            sys.exit(1)
        self._print_log(log=log, previous_log_len=previous_log_len)
        logger.info(f"Job completed with execution status {status}")
        if data_access is None:
            return
        # Here data access is either S3 or lakehouse both of which contain self.output_folder
        try:
            output_folder = data_access.output_folder
        except Exception as e:
            logger.warning(f"failed to get output folder {e}")
            return
        output_folder = output_folder if output_folder.endswith("/") else output_folder + "/"
        execution_log_path = f"{output_folder}execution.log"
        logger.info(f"saving execution log to {execution_log_path}")
        data_access.save_file(path=execution_log_path, data=bytes(log, "UTF-8"))


class ComponentUtils:
    """
    Class containing methods supporting building pipelines
    """

    # @staticmethod
    # def add_settings_to_component(
    #     task: dsl.PipelineTask,
    #     timeout: int,
    #     image_pull_policy: str = "IfNotPresent",
    #     cache_strategy: bool = False,
    # ) -> None:
    #     """
    #     Add settings to kfp task
    #     :param task: kfp task
    #     :param timeout: timeout to set to the component in seconds
    #     :param image_pull_policy: pull policy to set to the component
    #     :param cache_strategy: cache strategy
    #     """
    #
    #     kubernetes.use_field_path_as_env(task, env_name=RUN_NAME, field_path="metadata.annotations['pipelines.kubeflow.org/run_name']")
    #     # Set cashing
    #     task.set_caching_options(enable_caching=cache_strategy)
    #     # image pull policy
    #     kubernetes.set_image_pull_policy(task, image_pull_policy)
    #     # Set the timeout for the task to one day (in seconds)
    #     kubernetes.set_timeout(task, seconds=timeout)


    @staticmethod
    def default_compute_execution_params(
        worker_options: str,  # ray worker configuration
        actor_options: str,  # cpus per actor
    ) -> str:
        """
        This is the most simplistic transform execution parameters computation
        :param worker_options: configuration of ray workers
        :param actor_options: actor request requirements
        :return: number of actors
        """
        import sys

        from data_processing.utils import get_logger
        from kfp_support.workflow_support.runtime_utils import KFPUtils

        logger = get_logger(__name__)

        # convert input
        w_options = KFPUtils.load_from_json(worker_options.replace("'", '"'))
        a_options = KFPUtils.load_from_json(actor_options.replace("'", '"'))
        # Compute available cluster resources
        cluster_cpu = w_options["replicas"] * w_options["cpu"]
        cluster_mem = w_options["replicas"] * w_options["memory"]
        cluster_gpu = w_options["replicas"] * w_options.get("gpu", 0.0)
        logger.info(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_mem}, GPUs {cluster_gpu}")
        # compute number of actors
        n_actors_cpu = int(cluster_cpu * 0.85 / a_options.get("num_cpus", 0.5))
        n_actors_memory = int(cluster_mem * 0.85 / a_options.get("memory", 1))
        n_actors = min(n_actors_cpu, n_actors_memory)
        # Check if we need gpu calculations as well
        actor_gpu = a_options.get("num_gpus", 0)
        if actor_gpu > 0:
            n_actors_gpu = int(cluster_gpu / actor_gpu)
            n_actors = min(n_actors, n_actors_gpu)
        logger.info(f"Number of actors - {n_actors}")
        if n_actors < 1:
            logger.warning(
                f"Not enough cpu/gpu/memory to run transform, "
                f"required cpu {a_options.get('num_cpus', .5)}, available {cluster_cpu}, "
                f"required memory {a_options.get('memory', 1)}, available {cluster_mem}, "
                f"required cpu {actor_gpu}, available {cluster_gpu}"
            )
            sys.exit(1)

        return str(n_actors)
