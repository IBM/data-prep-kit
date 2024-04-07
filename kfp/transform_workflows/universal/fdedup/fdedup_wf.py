import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
from kfp_support.workflow_support.utils import (
    ONE_HOUR_SEC,
    ONE_WEEK_SEC,
    ComponentUtils,
)
from kubernetes import client as k8s_client
from src.fdedup_compute_execution_params import fdedup_compute_execution_params

# the name of the job script
EXEC_SCRIPT_NAME: str = "fdedup_transform.py"

# components
base_kfp_image = "us.icr.io/cil15-shared-registry/preprocessing-pipelines/kfp-data-processing:0.0.3"
# compute execution parameters
compute_exec_params_op = comp.func_to_container_op(
    func=fdedup_compute_execution_params, base_image=base_kfp_image
)
# create Ray cluster
create_ray_op = comp.load_component_from_file("../../../kfp_ray_components/createRayComponent.yaml")
# execute job
execute_ray_jobs_op = comp.load_component_from_file("../../../kfp_ray_components/executeRayJobComponent.yaml")
# clean up Ray
cleanup_ray_op = comp.load_component_from_file("../../../kfp_ray_components/cleanupRayComponent.yaml")
# Task name is part of the pipeline name, the ray cluster name and the job name in DMF.
TASK_NAME: str = "fdedup"


@dsl.pipeline(
    name=TASK_NAME + "-ray-pipeline",
    description="Pipeline for fdedup",
)
def fdedup(
    # Ray cluster
    ray_name: str = "fdedup-kfp-ray",  # name of Ray cluster
    ray_head_options: str = '{"cpu": 1, "memory": 4, "image": "us.icr.io/cil15-shared-registry/preprocessing-pipelines/fdedup:guftest",\
             "image_pull_secret": "prod-all-icr-io"}',
    ray_worker_options: str = '{"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, "image_pull_secret": "prod-all-icr-io",\
            "image": "us.icr.io/cil15-shared-registry/preprocessing-pipelines/fdedup:guftest"}',
    server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    # data access. checkpointing is not supported by dedup
    data_lh_config: str = "None",
    data_s3_config: str = "{'input_folder': 'cos-optimal-llm-pile/sanity-test/input/dataset=text/', 'output_folder': 'cos-optimal-llm-pile/doc_annotation_test/output_blocklist_guf/'}",
    data_s3_access_secret: str = "cos-access",
    data_max_files: int = -1,
    data_num_samples: int = -1,
    # orchestrator
    actor_options: str = "{'num_cpus': 0.8}",
    pipeline_id: str = "pipeline_id",
    code_location: str = "{'github': 'github', 'commit_hash': '12345', 'path': 'path'}",
    # columns used
    doc_column: str = "contents",
    id_column: str = "int_id_column",
    cluster_column: str =  "cluster",
    # infrastructure
    bucket_cpu: float = 0.5,
    doc_cpu: float = 0.5,
    mhash_cpu: float = 0.5,
    # fuzzy parameters
    num_permutations: int = 64,
    threshold: float = 0.8,
    shingles_size: int = 5,
    japanese_data: bool = False,
    delimiters: str = " ",
    # additional parameters
    additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5}',
):
    """
    Pipeline to execute FDEDUP transform
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
    :param data_lh_config - lake house configuration
    :param data_s3_access_secret - s3 access secret
    :param data_s3_config - s3 configuration
    :param data_max_files - max files to process
    :param data_num_samples - num samples to process
    :param actor_options - actor options
    :param pipeline_id - pipeline id
    :param code_location - code location
    :param doc_column - document column name
    :param id_column - integer document id column name
    :param cluster_column - cluster column name
    :param bucket_cpu - number of CPUs per bucket hash
    :param doc_cpu - number of CPUs per doc hash
    :param mhash_cpu - number of CPUs per minhash hash
    :param num_permutations - number of permutations
    :param threshold - threshold
    :param shingles_size - number of words in shingle
    :param japanese_data - japanese data indicator
    :param delimiters - delimiter for splitting document
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
            actor_options=actor_options,
            params={"threshold": threshold, "num_permutations": num_permutations,
                    "s3_config": data_s3_config, "bucket_cpu": bucket_cpu,
                    "doc_cpu": doc_cpu, "minhash_cpu": mhash_cpu}
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
                "data_lh_config": data_lh_config,
                "data_max_files": data_max_files,
                "data_num_samples": data_num_samples,
                "num_workers": compute_exec_params.outputs["workers"],
                "worker_options": actor_options,
                "pipeline_id": pipeline_id,
                "job_id": dsl.RUN_ID_PLACEHOLDER,
                "code_location": code_location,
                "doc_column": doc_column,
                "id_column": id_column,
                "cluster_column": cluster_column,
                "bucket_cpu": bucket_cpu,
                "doc_cpu": doc_cpu,
                "mhash_cpu": mhash_cpu,
                "num_doc_actors": compute_exec_params.outputs["docs"],
                "num_bucket_actors": compute_exec_params.outputs["buckets"],
                "num_minhash_actors": compute_exec_params.outputs["min_hashes"],
                "num_preprocessors": compute_exec_params.outputs["preprocessors"],
                "num_permutations": num_permutations,
                "threshold": threshold,
                "shingles_size": shingles_size,
                "japanese_data": japanese_data,
                "delimiters": delimiters,
            },
            exec_script_name=EXEC_SCRIPT_NAME,
            server_url=server_url,
        )
        ComponentUtils.add_settings_to_component(execute_job, ONE_WEEK_SEC)
        ComponentUtils.set_s3_env_vars_to_component(execute_job, data_s3_access_secret)
        execute_job.after(ray_cluster)

    # set image pull secrets
    dsl.get_pipeline_conf().set_image_pull_secrets([k8s_client.V1ObjectReference(name="prod-all-icr-io")])
    # Configure the pipeline level to one week (in seconds)
    dsl.get_pipeline_conf().set_timeout(ONE_WEEK_SEC)


if __name__ == "__main__":
    # Compiling the pipeline
    compiler.Compiler().compile(fdedup, __file__.replace(".py", ".yaml"))
