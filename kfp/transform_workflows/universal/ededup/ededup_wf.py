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

import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
from kfp_support.workflow_support.utils import (
    ONE_HOUR_SEC,
    ONE_WEEK_SEC,
    ComponentUtils,
)
from src.ededup_compute_execution_params import ededup_compute_execution_params


# the name of the job script
EXEC_SCRIPT_NAME: str = "ededup_transform.py"

task_image = "quay.io/dataprep1/data-prep-lab/ededup:0.2.1"

# components
base_kfp_image = "quay.io/dataprep1/data-prep-lab/kfp-data-processing:0.0.7"

# compute execution parameters
compute_exec_params_op = comp.func_to_container_op(func=ededup_compute_execution_params, base_image=base_kfp_image)
# create Ray cluster
create_ray_op = comp.load_component_from_file("../../../kfp_ray_components/createRayComponent.yaml")
# execute job
execute_ray_jobs_op = comp.load_component_from_file("../../../kfp_ray_components/executeRayJobComponent.yaml")
# clean up Ray
cleanup_ray_op = comp.load_component_from_file("../../../kfp_ray_components/cleanupRayComponent.yaml")
# Task name is part of the pipeline name, the ray cluster name and the job name in DMF.
TASK_NAME: str = "ededup"


@dsl.pipeline(
    name=TASK_NAME + "-ray-pipeline",
    description="Pipeline for ededup",
)
def ededup(
    # Ray cluster
    ray_name: str = "ededup-kfp-ray",  # name of Ray cluster
    ray_head_options: str = '{"cpu": 1, "memory": 4, "image_pull_secret": "", "image": "' + task_image + '" }',
    ray_worker_options: str = '{"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, '
    '"image_pull_secret": "", "image": "' + task_image + '"}',
    server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    # data access. checkpointing is not supported by dedup
    data_s3_config: str = "{'input_folder': 'test/ededup/input/', 'output_folder': 'test/ededup/output'}",
    data_s3_access_secret: str = "s3-secret",
    data_max_files: int = -1,
    data_num_samples: int = -1,
    # orchestrator
    runtime_actor_options: str = "{'num_cpus': 0.8}",
    runtime_pipeline_id: str = "pipeline_id",
    runtime_code_location: str = "{'github': 'github', 'commit_hash': '12345', 'path': 'path'}",
    # ededup
    ededup_hash_cpu: float = 0.5,
    ededup_doc_column: str = "contents",
    # data sampling
    ededup_n_samples: int = 10,
    # additional parameters
    additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5}',
):
    """
    Pipeline to execute EDEDUP transform
    :param ray_name: name of the Ray cluster
    :param ray_head_options: head node options, containing the following:
        cpu - number of cpus
        memory - memory
        image - image to use
        image_pull_secret - image pull secret
    :param ray_worker_options: worker node options (we here are using only 1 worker pool), containing the following:
        replicas - number of replicas to create
        max_replicas - max number of replicas
        min_replicas - min number of replicas
        cpu - number of cpus
        memory - memory
        image - image to use
        image_pull_secret - image pull secret
    :param server_url - server url
    :param additional_params: additional (support) parameters, containing the following:
        wait_interval - wait interval for API server, sec
        wait_cluster_ready_tmout - time to wait for cluster ready, sec
        wait_cluster_up_tmout - time to wait for cluster up, sec
        wait_job_ready_tmout - time to wait for job ready, sec
        wait_print_tmout - time between prints, sec
        http_retries - http retries for API server calls
    :param data_s3_access_secret - s3 access secret
    :param data_s3_config - s3 configuration
    :param data_max_files - max files to process
    :param data_num_samples - num samples to process
    :param runtime_actor_options - actor options
    :param runtime_pipeline_id - pipeline id
    :param runtime_code_location - code location
    :param ededup_hash_cpu - number of CPUs per hash
    :param ededup_doc_column - key for accessing data
    :param ededup_n_samples - number of samples for parameters computation
    :return: None
    """
    # create clean_up task
    clean_up_task = cleanup_ray_op(ray_name=ray_name, run_id=dsl.RUN_ID_PLACEHOLDER, server_url=server_url)
    ComponentUtils.add_settings_to_component(clean_up_task, 60)
    # pipeline definition
    with dsl.ExitHandler(clean_up_task):
        # compute execution params
        compute_exec_params = compute_exec_params_op(
            worker_options=ray_worker_options,
            actor_options=runtime_actor_options,
            params={"s3_config": data_s3_config, "hash_cpu": ededup_hash_cpu},
            n_samples=ededup_n_samples,
        )
        ComponentUtils.add_settings_to_component(compute_exec_params, ONE_HOUR_SEC * 2)
        ComponentUtils.set_s3_env_vars_to_component(compute_exec_params, data_s3_access_secret)

        # start Ray cluster
        ray_cluster = create_ray_op(
            ray_name=ray_name,
            run_id=dsl.RUN_ID_PLACEHOLDER,
            ray_head_options=ray_head_options,
            ray_worker_options=ray_worker_options,
            server_url=server_url,
            additional_params=additional_params,
        )
        ComponentUtils.add_settings_to_component(ray_cluster, ONE_HOUR_SEC * 2)
        ray_cluster.after(compute_exec_params)
        # Execute job
        execute_job = execute_ray_jobs_op(
            ray_name=ray_name,
            run_id=dsl.RUN_ID_PLACEHOLDER,
            additional_params=additional_params,
            exec_params={
                "data_s3_config": data_s3_config,
                "data_max_files": data_max_files,
                "data_num_samples": data_num_samples,
                "runtime_num_workers": compute_exec_params.outputs["workers"],
                "runtime_worker_options": runtime_actor_options,
                "runtime_pipeline_id": runtime_pipeline_id,
                "runtime_job_id": dsl.RUN_ID_PLACEHOLDER,
                "runtime_code_location": runtime_code_location,
                "ededup_doc_column": ededup_doc_column,
                "ededup_hash_cpu": ededup_hash_cpu,
                "ededup_num_hashes": compute_exec_params.outputs["hashes"],
            },
            exec_script_name=EXEC_SCRIPT_NAME,
            server_url=server_url,
        )
        ComponentUtils.add_settings_to_component(execute_job, ONE_WEEK_SEC)
        ComponentUtils.set_s3_env_vars_to_component(execute_job, data_s3_access_secret)
        execute_job.after(ray_cluster)

    # Configure the pipeline level to one week (in seconds)
    dsl.get_pipeline_conf().set_timeout(ONE_WEEK_SEC)


if __name__ == "__main__":
    # Compiling the pipeline
    compiler.Compiler().compile(ededup, __file__.replace(".py", ".yaml"))
