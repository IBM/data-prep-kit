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

import re
import sys
import time
from typing import Any

from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.utils import ParamsUtils, get_logger
from python_apiserver_client import KubeRayAPIs
from python_apiserver_client.params import (
    DEFAULT_HEAD_START_PARAMS,
    DEFAULT_WORKER_START_PARAMS,
    Cluster,
    ClusterSpec,
    HeadNodeSpec,
    RayJobRequest,
    WorkerNodeSpec,
    environment_variables_decoder,
    template_decoder,
    volume_decoder,
)
from ray.job_submission import JobStatus
from runtime_utils import KFPUtils


cli_prefix = "KFP"

logger = get_logger(__name__)


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
                image_pull_policy: image pull policy, default IfNotPresent

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
                image_pull_policy: image pull policy, default IfNotPresent

        :param wait_cluster_ready - time to wait for cluster ready sec (-1 forever)
        :return:tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # start with templates
        # head_node
        dct = {}
        dct["cpu"] = head_node.get("cpu", 1)
        dct["memory"] = head_node.get("memory", 1)
        dct["gpu"] = head_node.get("gpu", 0)
        dct["gpu_accelerator"] = head_node.get("gpu_accelerator", None)
        head_node_template_name = f"{name}-head-template"
        dct["name"] = head_node_template_name
        dct["namespace"] = namespace
        if "tolerations" in head_node:
            dct["tolerations"] = head_node.get("tolerations")
        _, _ = self.api_server_client.delete_compute_template(ns=namespace, name=dct["name"])
        head_template = template_decoder(dct)
        status, error = self.api_server_client.create_compute_template(head_template)
        if status != 200:
            return status, error
        worker_template_names = [""] * len(worker_nodes)
        index = 0
        # For every worker group
        for worker_node in worker_nodes:
            dct = {}
            dct["cpu"] = worker_node.get("cpu", 1)
            dct["memory"] = worker_node.get("memory", 1)
            dct["gpu"] = worker_node.get("gpu", 0)
            dct["gpu_accelerator"] = worker_node.get("gpu_accelerator", None)
            worker_node_template_name = f"{name}-worker-template-{index}"
            dct["name"] = worker_node_template_name
            dct["namespace"] = namespace
            if "tolerations" in worker_node:
                dct["tolerations"] = worker_node.get("tolerations")
            _, _ = self.api_server_client.delete_compute_template(ns=namespace, name=dct["name"])
            worker_template = template_decoder(dct)
            status, error = self.api_server_client.create_compute_template(worker_template)
            if status != 200:
                return status, error
            worker_template_names[index] = worker_node_template_name
            index += 1
        # Build head node spec
        image = head_node.get("image", self.default_image)
        image_pull_secret = head_node.get("image_pull_secret", None)
        image_pull_policy = head_node.get("image_pull_policy", None)
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
            image_pull_policy=image_pull_policy,
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
            image_pull_policy = head_node.get("image_pull_policy", None)
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
                    image_pull_policy=image_pull_policy,
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
        # Although the cluster is ready, the service web server might not be ready yet at this point.
        # To ensure that it is ready, trying to get jobs info from the cluster. Even if it fails
        # couple of times, its harmless
        _, _, _ = self.api_server_client.list_job_info(ns=namespace, name=name)
        time.sleep(5)
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
        logger.info(f"Job completed with execution status {job_status}")
        if job_status != JobStatus.SUCCEEDED:
            sys.exit(1)
        if data_access is None:
            return
        # Here data access is S3 which contains self.output_folder
        try:
            output_folder = data_access.get_output_folder()
        except Exception as e:
            logger.warning(f"failed to get output folder {e}")
            return
        output_folder = output_folder if output_folder.endswith("/") else output_folder + "/"
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        execution_log_path = f"{output_folder}execution_{timestamp}.log"
        logger.info(f"saving execution log to {execution_log_path}")
        data_access.save_file(path=execution_log_path, data=bytes(log, "UTF-8"))


def _execute_remote_job(
    name: str,
    ns: str,
    script: str,
    params: dict[str, Any],
    data_access_params: dict[str, Any],
    additional_params: dict[str, Any],
    remote_jobs: RayRemoteJobs,
) -> None:
    """
    Execute remote job on Ray cluster
    :param name: cluster name
    :param ns: execution/cluster namespace
    :param additional_params: additional parameters for the job
    :param data_access_params: data access parameters
    :param params: job execution parameters (specific for a specific transform,
                        generated by the transform workflow)
    :param script: script to run (has to be present in the image)
    :param remote_jobs: remote jobs execution support class
    :return:
    """

    status, error, submission = remote_jobs.submit_job(name=name, namespace=ns, request=params, executor=script)
    if status != 200:
        logger.error(f"Failed to submit job - status: {status}, error: {error}")
        exit(1)

    logger.info(f"submitted job successfully, submission id {submission}")
    # create data access
    data_factory = DataAccessFactory(cli_arg_prefix=cli_prefix)
    data_factory.apply_input_params(args=data_access_params)
    data_access = data_factory.create_data_access()
    # print execution log
    remote_jobs.follow_execution(
        name=name,
        namespace=ns,
        submission_id=submission,
        data_access=data_access,
        print_timeout=additional_params.get("wait_print_tmout", 120),
        job_ready_timeout=additional_params.get("wait_job_ready_tmout", 600),
    )


def execute_ray_jobs(
    name: str,  # name of Ray cluster
    additional_params: dict[str, Any],
    e_params: dict[str, Any],
    exec_script_name: str,
    server_url: str,
) -> None:
    """
    Execute Ray jobs on a cluster periodically printing execution log. Completes when all Ray job complete.
    All of the jobs will be executed, although some of the jobs may fail.
    :param name: cluster name
    :param additional_params: additional parameters for the job
    :param e_params: job execution parameters (specific for a specific transform,
                        generated by the transform workflow)
    :param exec_script_name: script to run (has to be present in the image)
    :param server_url: API server url
    :return: None
    """
    # prepare for execution
    ns = KFPUtils.get_namespace()
    if ns == "":
        logger.warning(f"Failed to get namespace")
        sys.exit(1)
    # create remote jobs class
    remote_jobs = RayRemoteJobs(
        server_url=server_url,
        http_retries=additional_params.get("http_retries", 5),
        wait_interval=additional_params.get("wait_interval", 2),
    )
    # find config parameter
    config = ParamsUtils.get_config_parameter(params=e_params)
    if config is None:
        exit(1)
    # get config value
    config_value = KFPUtils.load_from_json(e_params[config].replace("'", '"'))
    s3_creds = KFPUtils.load_from_json(e_params["data_s3_cred"].replace("'", '"'))
    if type(config_value) is not list:
        # single request
        return _execute_remote_job(
            name=name,
            ns=ns,
            script=exec_script_name,
            data_access_params={f"{cli_prefix}s3_config": config_value, f"{cli_prefix}s3_cred": s3_creds},
            params=e_params,
            additional_params=additional_params,
            remote_jobs=remote_jobs,
        )
    # remove config key from the dictionary
    launch_params = dict(e_params)
    del launch_params[config]
    # Loop through all configuration
    n_launches = 0
    for conf in config_value:
        # populate individual config and launch
        launch_params[config] = ParamsUtils.convert_to_ast(d=conf)
        try:
            _execute_remote_job(
                name=name,
                ns=ns,
                script=exec_script_name,
                data_access_params={f"{cli_prefix}s3_config": conf, f"{cli_prefix}s3_cred": s3_creds},
                params=launch_params,
                additional_params=additional_params,
                remote_jobs=remote_jobs,
            )
            n_launches += 1
        except SystemExit:
            logger.warning(f"Failed to execute job for configuration {conf}")
            continue

    if n_launches == 0:
        logger.warning("All executions failed")
        sys.exit(1)
    else:
        logger.info(f"{n_launches} ot of {len(config_value)} succeeded")
