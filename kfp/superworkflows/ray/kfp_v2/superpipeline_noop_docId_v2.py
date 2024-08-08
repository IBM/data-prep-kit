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
    d.pop("input_parent_path", None)
    d.pop("output_parent_path", None)
    d.pop("parent_path_suffix", None)
    d.pop("inter_output", None)
    d.pop("skip", None)
    d.pop("name", None)
    d.pop("overriding_params", None)
    return


@dsl.component
def prepare_params(input_prefix: str, inter_output_path: str) -> str:
    data_s3_config = "{'input_folder': '" + input_prefix + "', 'output_folder': '" + inter_output_path + "'}"
    return data_s3_config


@dsl.pipeline
def super_pipeline(
    # the super pipeline parameters
    p1_pipeline_runtime_pipeline_id: str = "pipeline_id",
    p1_pipeline_server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    p1_pipeline_input_parent_path: str = "test/doc_id/input/",
    p1_pipeline_output_parent_path: str = "test/super/output/",
    # p1_pipeline_parent_path_suffix: str = "",
    p1_pipeline_inter_output: str = "test/super/output/tmp",
    p1_pipeline_additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5}',
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
    # overriding parameters
    p2_overriding_params: dict = {"ray_worker_options": {"image": noop_image}, "ray_head_options": {"image": noop_image}},
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
):
    args = locals()
    common_params = {key: value for key, value in args.items() if key.startswith("p1_")}
    task1_params = {key: value for key, value in args.items() if key.startswith("p2_")}
    task2_params = {key: value for key, value in args.items() if key.startswith("p3_")}
    common_params_prefix = "p1_pipeline_"
    common_prms_to_pass = {key[len(common_params_prefix) :]: value for key, value in common_params.items()}

    #########################################
    prefix1 = "p2_"
    prefix2 = "p3_"
    pipeline_prms_to_pass = common_prms_to_pass | {key[len(prefix1) :]: value for key, value in task1_params.items()}

    _remove_unused_params(pipeline_prms_to_pass)

    input_prefix = common_params.get("p1_pipeline_input_parent_path", "")
    parent_path_suffix = common_params.get("p1_pipeline_parent_path_suffix", "")
    output_parent_path = common_params.get("p1_pipeline_output_parent_path", "")
    inter_output_path = common_params.get("p1_pipeline_inter_output", "")

    data_config = prepare_params(input_prefix=input_prefix, inter_output_path=inter_output_path)
    pipeline_prms_to_pass["data_s3_config"] = data_config.output
    noop_task = noop(**pipeline_prms_to_pass)
    #########################################
    
    pipeline_prms_to_pass2 = common_prms_to_pass | {key[len(prefix2) :]: value for key, value in task2_params.items()}
    _remove_unused_params(pipeline_prms_to_pass2)
    data_config = prepare_params(input_prefix=inter_output_path, inter_output_path=output_parent_path)
    pipeline_prms_to_pass2["data_s3_config"] = data_config.output
    doc_id_task = doc_id(**pipeline_prms_to_pass2)
    doc_id_task.after(noop_task)
    

if __name__ == "__main__":
    # Compiling the pipeline
    compiler.Compiler().compile(super_pipeline, __file__.replace(".py", ".yaml"))
