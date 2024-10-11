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
import os

import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
from workflow_support.compile_utils import ONE_HOUR_SEC, ONE_WEEK_SEC, ComponentUtils


task_image = "quay.io/dataprep1/data-prep-kit/doc_id-ray:latest"

# the name of the job script
EXEC_SCRIPT_NAME: str = "doc_id_transform_ray.py"
# components
base_kfp_image = "quay.io/dataprep1/data-prep-kit/kfp-data-processing:latest"

# path to kfp component specifications files
component_spec_path = "../../../../kfp/kfp_ray_components/"

# compute execution parameters. Here different transforms might need different implementations. As
# a result, instead of creating a component we are creating it in place here.
def compute_exec_params_func(
    worker_options: dict,
    actor_options: dict,
    data_s3_config: str,
    data_max_files: int,
    data_num_samples: int,
    data_checkpointing: bool,
    data_data_sets: str,
    data_files_to_use: str,
    runtime_pipeline_id: str,
    runtime_job_id: str,
    runtime_code_location: dict,
    doc_id_doc_column: str,
    doc_id_hash_column: str,
    doc_id_int_column: str,
    doc_id_start_id: int,
) -> dict:
    from runtime_utils import KFPUtils

    return {
        "data_s3_config": data_s3_config,
        "data_max_files": data_max_files,
        "data_num_samples": data_num_samples,
        "data_checkpointing": data_checkpointing,
        "data_data_sets": data_data_sets.strip(),
        "data_files_to_use": data_files_to_use,
        "runtime_num_workers": KFPUtils.default_compute_execution_params(str(worker_options), str(actor_options)),
        "runtime_worker_options": str(actor_options),
        "runtime_pipeline_id": runtime_pipeline_id,
        "runtime_job_id": runtime_job_id,
        "runtime_code_location": str(runtime_code_location),
        "doc_id_doc_column": doc_id_doc_column,
        "doc_id_hash_column": doc_id_hash_column,
        "doc_id_int_column": doc_id_int_column,
        "doc_id_start_id": doc_id_start_id,
    }


# KFPv1 and KFP2 uses different methods to create a component from a function. KFPv1 uses the
# `create_component_from_func` function, but it is deprecated by KFPv2 and so has a different import path.
# KFPv2 recommends using the `@dsl.component` decorator, which doesn't exist in KFPv1. Therefore, here we use
# this if/else statement and explicitly call the decorator.
if os.getenv("KFPv2", "0") == "1":
    # In KFPv2 dsl.RUN_ID_PLACEHOLDER is deprecated and cannot be used since SDK 2.5.0. On another hand we cannot create
    # a unique string in a component (at runtime) and pass it to the `clean_up_task` of `ExitHandler`, due to
    # https://github.com/kubeflow/pipelines/issues/10187. Therefore, meantime we use a unique string created at
    # compilation time.
    import uuid

    compute_exec_params_op = dsl.component_decorator.component(
        func=compute_exec_params_func, base_image=base_kfp_image
    )
    print(
        "WARNING: the ray cluster name can be non-unique at runtime, please do not execute simultaneous Runs of the "
        + "same version of the same pipeline !!!"
    )
    run_id = uuid.uuid4().hex
else:
    compute_exec_params_op = comp.create_component_from_func(func=compute_exec_params_func, base_image=base_kfp_image)
    run_id = dsl.RUN_ID_PLACEHOLDER

# create Ray cluster
create_ray_op = comp.load_component_from_file(component_spec_path + "createRayClusterComponent.yaml")
# execute job
execute_ray_jobs_op = comp.load_component_from_file(component_spec_path + "executeRayJobComponent.yaml")
# clean up Ray
cleanup_ray_op = comp.load_component_from_file(component_spec_path + "deleteRayClusterComponent.yaml")
# Task name is part of the pipeline name, the ray cluster name and the job name in DMF.
TASK_NAME: str = "doc_id"


@dsl.pipeline(
    name=TASK_NAME + "-ray-pipeline",
    description="Pipeline for doc_id",
)
def doc_id(
    # Ray cluster
    ray_name: str = "doc_id-kfp-ray",  # name of Ray cluster
    # Add image_pull_secret and image_pull_policy to ray workers if needed
    ray_head_options: dict = {"cpu": 1, "memory": 4, "image": task_image},
    ray_worker_options: dict = {"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, "image": task_image},
    server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    # data access
    data_s3_config: str = "{'input_folder': 'test/doc_id/input/', 'output_folder': 'test/doc_id/output/'}",
    data_s3_access_secret: str = "s3-secret",
    data_max_files: int = -1,
    data_num_samples: int = -1,
    data_checkpointing: bool = False,
    data_data_sets: str = "",
    data_files_to_use: str = "['.parquet']",
    # orchestrator
    runtime_actor_options: dict = {'num_cpus': 0.8},
    runtime_pipeline_id: str = "pipeline_id",
    runtime_code_location: dict = {'github': 'github', 'commit_hash': '12345', 'path': 'path'},
    # doc id parameters
    doc_id_doc_column: str = "contents",
    doc_id_hash_column: str = "hash_column",
    doc_id_int_column: str = "int_id_column",
    doc_id_start_id: int = 0,
    # additional parameters
    additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5, "delete_cluster_delay_minutes": 0}',
):
    """
    Pipeline to execute NOOP transform
    :param ray_name: name of the Ray cluster
    :param ray_head_options: head node options, containing the following:
        cpu - number of cpus
        memory - memory
        image - image to use
        image_pull_secret - image pull secret
        tolerations - (optional) tolerations for the ray pods
    :param ray_worker_options: worker node options (we here are using only 1 worker pool), containing the following:
        replicas - number of replicas to create
        max_replicas - max number of replicas
        min_replicas - min number of replicas
        cpu - number of cpus
        memory - memory
        image - image to use
        image_pull_secret - image pull secret
        tolerations - (optional) tolerations for the ray pods
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
    :param doc_id_doc_column - document column
    :param doc_id_hash_column - hash id column
    :param doc_id_int_column - integer id column
    :param doc_id_start_id - starting id
    :return: None
    """
    # create clean_up task
    clean_up_task = cleanup_ray_op(ray_name=ray_name, run_id=run_id, server_url=server_url, additional_params=additional_params)
    ComponentUtils.add_settings_to_component(clean_up_task, ONE_HOUR_SEC * 2)
    # pipeline definition
    with dsl.ExitHandler(clean_up_task):
        # compute execution params
        compute_exec_params = compute_exec_params_op(
            worker_options=ray_worker_options,
            actor_options=runtime_actor_options,
            data_s3_config=data_s3_config,
            data_max_files=data_max_files,
            data_num_samples=data_num_samples,
            data_checkpointing=data_checkpointing,
            data_data_sets=data_data_sets,
            data_files_to_use=data_files_to_use,
            runtime_pipeline_id=runtime_pipeline_id,
            runtime_job_id=run_id,
            runtime_code_location=runtime_code_location,
            doc_id_doc_column=doc_id_doc_column,
            doc_id_hash_column=doc_id_hash_column,
            doc_id_int_column=doc_id_int_column,
            doc_id_start_id=doc_id_start_id
        )
        ComponentUtils.add_settings_to_component(compute_exec_params, ONE_HOUR_SEC * 2)
        # start Ray cluster
        ray_cluster = create_ray_op(
            ray_name=ray_name,
            run_id=run_id,
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
            run_id=run_id,
            additional_params=additional_params,
            # note that the parameters below are specific for NOOP transform
            exec_params=compute_exec_params.output,
            exec_script_name=EXEC_SCRIPT_NAME,
            server_url=server_url,
        )
        ComponentUtils.add_settings_to_component(execute_job, ONE_WEEK_SEC)
        ComponentUtils.set_s3_env_vars_to_component(execute_job, data_s3_access_secret)
        execute_job.after(ray_cluster)


if __name__ == "__main__":
    # Compiling the pipeline
    compiler.Compiler().compile(doc_id, __file__.replace(".py", ".yaml"))
