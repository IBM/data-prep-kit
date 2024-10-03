from typing import Any, NamedTuple
import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
from kfp import dsl

from universal.noop.kfp_ray.noop_wf import noop
from universal.doc_id.kfp_ray.doc_id_wf import doc_id

noop_image = "quay.io/dataprep1/data-prep-kit/noop-ray:latest"
doc_id_image = "quay.io/dataprep1/data-prep-kit/doc_id-ray:latest"


def _remove_unused_params(d: dict[str, Any]) -> None:
    d.pop("input_path", None)
    d.pop("output_path", None)
    d.pop("intermediate_path", None)
    d.pop("skip", None)
    d.pop("name", None)
    d.pop("overriding_params", None)
    return


@dsl.component
def prepare_params(input_path: str, output_path: str) -> str:
    data_s3_config = "{'input_folder': '" + input_path + "', 'output_folder': '" + output_path + "'}"
    return data_s3_config


@dsl.pipeline
def super_pipeline(
    # the super pipeline parameters
    p1_pipeline_runtime_pipeline_id: str = "pipeline_id",
    p1_pipeline_server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    p1_pipeline_input_path: str = "test/doc_id/input/",
    p1_pipeline_output_path: str = "test/super/output/",
    p1_pipeline_intermediate_path: str = "test/super/output/tmp",
    p1_pipeline_additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5, "delete_cluster_delay_minutes": 0}',
    p1_pipeline_data_s3_access_secret: str = "s3-secret",
    p1_pipeline_runtime_code_location: dict = {"github": "github", "commit_hash": "12345", "path": "path"},
    p1_pipeline_runtime_actor_options: dict = {'num_cpus': 0.8},
    # data access
    p1_pipeline_data_max_files: int = -1,
    p1_pipeline_data_num_samples: int = -1,
    p1_pipeline_data_checkpointing: bool = False,
    # noop step parameters
    p2_name: str = "noop",
    p2_skip: bool = False,
    p2_noop_sleep_sec: int = 10,
    p2_ray_name: str = "noop-kfp-ray",
    p2_ray_head_options: dict = {"cpu": 1, "memory": 4, "image_pull_secret": "", "image": noop_image},
    p2_ray_worker_options: dict = {"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, "image_pull_secret": "", "image": noop_image},
    # Document ID step parameters
    p3_name: str = "doc_id",
    p3_ray_name: str = "docid-kfp-ray",
    p3_ray_head_options: dict = {"cpu": 1, "memory": 4, "image_pull_secret": "", "image": doc_id_image},
    p3_ray_worker_options: dict = {"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, "image_pull_secret": "", "image": doc_id_image},
    # p3_skip: bool = False,
    # orchestrator
    p3_data_data_sets: str = "",
    p3_data_files_to_use: str = "['.parquet']",
    # doc id parameters
    p3_doc_id_doc_column: str = "contents",
    p3_doc_id_hash_column: str = "hash_column",
    p3_doc_id_int_column: str = "int_id_column",
    p3_doc_id_start_id: int = 0,
):
    args = locals()
    common_params_prefix = "p1_pipeline_"
    transform1_prefix = "p2_"
    transform2_prefix = "p3_"
    # split the input parameters according to thier prefixes.
    common_params = {key[len(common_params_prefix) :]: value for key, value in args.items() if key.startswith(common_params_prefix)}
    task1_params = {key[len(transform1_prefix) :]: value for key, value in args.items() if key.startswith(transform1_prefix)}
    task2_params = {key[len(transform2_prefix) :]: value for key, value in args.items() if key.startswith(transform2_prefix)}

    # get the input path, output path of the whole pipeline, and the intermediate path for storing the files between the transforms
    input_path = common_params.get("input_path", "")
    output_path = common_params.get("output_path", "")
    inter_path = common_params.get("intermediate_path", "")

    # execute the first transform
    pipeline_prms_to_pass = common_params | task1_params
    _remove_unused_params(pipeline_prms_to_pass)
    # get the data config
    data_config = prepare_params(input_path=input_path, output_path=inter_path)
    pipeline_prms_to_pass["data_s3_config"] = data_config.output
    # call the noop pipeline from noop_wf.py file with the expected parameters
    noop_task = noop(**pipeline_prms_to_pass)

    # execute the second transform
    pipeline_prms_to_pass = common_params | task2_params
    _remove_unused_params(pipeline_prms_to_pass)
    # get the data config
    data_config = prepare_params(input_path=inter_path, output_path=output_path)
    pipeline_prms_to_pass["data_s3_config"] = data_config.output
    # call the doc_id pipeline from doc_id_wf.py file with the expected parameters
    doc_id_task = doc_id(**pipeline_prms_to_pass)
    doc_id_task.after(noop_task)
    

if __name__ == "__main__":
    # Compiling the pipeline
    compiler.Compiler().compile(super_pipeline, __file__.replace(".py", ".yaml"))
