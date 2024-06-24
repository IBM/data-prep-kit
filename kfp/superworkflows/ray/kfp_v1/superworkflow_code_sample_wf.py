import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
from kfp_support.workflow_support.utils import ONE_WEEK_SEC


# Components
# For every sub workflow we need a separate components, that knows about this subworkflow.
component_spec_path = "../../../kfp_ray_components/"
run_code_quality_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")
run_malware_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")
run_proglang_select_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")
run_doc_id_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")
run_exact_dedup_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")
run_fuzzy_dedup_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")
run_tokenization_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")

proglang_select_image = "quay.io/dataprep1/data-prep-kit/proglang_select-ray:0.2.0.dev6"
code_quality_image = "quay.io/dataprep1/data-prep-kit/code_quality-ray:0.2.0.dev6"
malware_image = "quay.io/dataprep1/data-prep-kit/malware-ray:0.2.0.dev6"
doc_id_image = "quay.io/dataprep1/data-prep-kit/doc_id-ray:0.2.0.dev6"
ededup_image = "quay.io/dataprep1/data-prep-kit/ededup-ray:0.2.0.dev6"
fdedup_image = "quay.io/dataprep1/data-prep-kit/fdedup-ray:0.2.0.dev6"
tokenizer_image = "quay.io/dataprep1/data-prep-kit/tokenization-ray:0.2.0.dev6"


# Pipeline to invoke execution on remote resource
@dsl.pipeline(
    name="super-kubeflow-pipeline-code",
    description="Super pipeline for programming languages data preprocessing",
)
def sample_code_ray_orchestrator(
    # the super pipeline parameters
    p1_orch_code_quality_name: str = "code_quality_wf",
    p1_orch_malware_name: str = "malware_wf",
    p1_orch_proglang_select_name: str = "proglang_select_wf",
    p1_orch_doc_id_name: str = "doc_id_wf",
    p1_orch_exact_dedup_name: str = "ededup_wf",
    p1_orch_fuzzy_dedup_name: str = "fdedup_wf",
    p1_orch_tokenization_wf_name: str = "tokenization_wf",
    p2_pipeline_runtime_pipeline_id: str = "pipeline_id",
    p2_pipeline_ray_head_options: str = '{"cpu": 1, "memory": 4, "image_pull_secret": ""}',
    p2_pipeline_ray_worker_options: str = '{"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, "image_pull_secret": ""}',
    p2_pipeline_server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    p2_pipeline_input_parent_path: str = "test/ingest2parquet/output/",
    p2_pipeline_output_parent_path: str = "test/super/output/",
    p2_pipeline_parent_path_suffix: str = "",
    p2_pipeline_additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5}',
    p2_pipeline_data_s3_access_secret: str = "s3-secret",
    p2_pipeline_runtime_code_location: str = '{"github": "github", "commit_hash": "12345", "path": "path"}',
    p2_pipeline_runtime_actor_options: str = '{"num_cpus": 0.8}',
    p2_pipeline_data_max_files: int = -1,
    p2_pipeline_data_num_samples: int = -1,
    # Exact dedup step parameters
    p3_name: str = "ededup",
    p3_skip: bool = False,
    p3_ededup_doc_column: str = "contents",
    p3_ededup_hash_cpu: float = 0.5,
    # data sampling
    p3_ededup_n_samples: int = 10,
    # overriding parameters
    p3_overriding_params: str = '{"ray_worker_options": {"image": "'
    + ededup_image
    + '"}, "ray_head_options": {"image": "'
    + ededup_image
    + '"}}',
    # Document ID step parameters
    p4_name: str = "doc_id",
    p4_skip: bool = False,
    # doc id parameters
    p4_doc_id_doc_column: str = "contents",
    p4_doc_id_hash_column: str = "hash_column",
    p4_doc_id_int_column: str = "int_id_column",
    # overriding parameters
    p4_overriding_params: str = '{"ray_worker_options": {"image": "'
    + doc_id_image
    + '"}, "ray_head_options": {"image": "'
    + doc_id_image
    + '"}}',
    # Fuzzy dedup step parameters
    p5_name: str = "fdedup",
    p5_skip: bool = False,
    # columns used
    p5_fdedup_doc_column: str = "contents",
    p5_fdedup_id_column: str = "int_id_column",
    p5_fdedup_cluster_column: str = "cluster",
    # orchestrator
    # infrastructure
    p5_fdedup_bucket_cpu: float = 0.5,
    p5_fdedup_doc_cpu: float = 0.5,
    p5_fdedup_mhash_cpu: float = 0.5,
    # fuzzy parameters
    p5_fdedup_num_permutations: int = 64,
    p5_fdedup_threshold: float = 0.8,
    p5_fdedup_shingles_size: int = 5,
    p5_fdedup_delimiters: str = " ",
    # random delay between reads
    p5_fdedup_random_delay_limit: int = 5,
    # snapshotting
    p5_fdedup_snapshot_delay: int = 1,
    p5_fdedup_use_doc_snapshot: bool = False,
    p5_fdedup_use_bucket_snapshot: bool = False,
    # data sampling
    p5_fdedup_n_samples: int = 10,
    # overriding parameters
    p5_overriding_params: str = '{"ray_worker_options": {"image": "'
    + fdedup_image
    + '"}, "ray_head_options": {"image": "'
    + fdedup_image
    + '"}}',
    # proglang_select step parameters
    p6_name: str = "proglang_select",
    p6_skip: bool = False,
    p6_proglang_select_allowed_langs_file: str = "test/proglang_select/languages/allowed-code-languages.txt",
    p6_proglang_select_language_column: str = "programming_language",
    p6_proglang_select_s3_access_secret: str = "s3-secret",
    # overriding parameters
    p6_overriding_params: str = '{"ray_worker_options": {"image": "'
    + proglang_select_image
    + '"}, "ray_head_options": {"image": "'
    + proglang_select_image
    + '"}}',
    # Code quality step parameters
    p7_name: str = "code_quality",
    p7_skip: bool = False,
    p7_cq_contents_column_name: str = "contents",
    p7_cq_language_column_name: str = "programming_language",
    p7_cq_tokenizer: str = "codeparrot/codeparrot",
    p7_cq_hf_token: str = "None",
    # orchestrator
    # overriding parameters
    p7_overriding_params: str = '{"ray_worker_options": {"image": "'
    + code_quality_image
    + '"}, "ray_head_options": {"image": "'
    + code_quality_image
    + '"}}',
    # malware step parameters
    p8_name: str = "malware",
    p8_skip: bool = False,
    p8_malware_input_column: str = "contents",
    p8_malware_output_column: str = "virus_detection",
    # orchestrator
    # overriding parameters
    p8_overriding_params: str = '{"ray_worker_options": {"image": "'
    + malware_image
    + '"}, "ray_head_options": {"image": "'
    + malware_image
    + '"}}',
    # tokenization parameters
    p9_name: str = "tokenization",
    p9_skip: bool = False,
    p9_tkn_tokenizer: str = "hf-internal-testing/llama-tokenizer",
    p9_tkn_doc_id_column: str = "document_id",
    p9_tkn_doc_content_column: str = "contents",
    p9_tkn_text_lang: str = "en",
    p9_tkn_tokenizer_args: str = "cache_dir=/tmp/hf",
    p9_tkn_chunk_size: int = 0,
    p9_overriding_params: str = '{"ray_worker_options": {"image": "'
    + tokenizer_image
    + '"}, "ray_head_options": {"image": "'
    + tokenizer_image
    + '"}}',
):

    # get all arguments
    args = locals()
    orch_host = "http://ml-pipeline:8888"

    def _set_component(op: dsl.BaseOp, displaied_name: str, prev_op: dsl.BaseOp = None):
        # set the sub component UI name
        op.set_display_name(displaied_name)

        # Add pod labels
        op.add_pod_label("app", "ml-pipeline").add_pod_label("component", "code-pipelines")
        # No cashing
        op.execution_options.caching_strategy.max_cache_staleness = "P0D"
        # image pull policy
        op.set_image_pull_policy("Always")
        # Set the timeout for each task to one week (in seconds)
        op.set_timeout(ONE_WEEK_SEC)
        if prev_op is not None:
            op.after(prev_op)

    # exact deduplication
    exact_dedup = run_exact_dedup_op(
        name=p1_orch_exact_dedup_name,
        prefix="p3_",
        params=args,
        host=orch_host,
        input_folder=p2_pipeline_input_parent_path,
    )
    _set_component(exact_dedup, "exact dedup")
    # document ID
    doc_id = run_doc_id_op(
        name=p1_orch_doc_id_name, prefix="p4_", params=args, host=orch_host, input_folder=exact_dedup.output
    )
    _set_component(doc_id, "doc ID", exact_dedup)
    # fuzzy deduplication
    fuzzy_dedup = run_fuzzy_dedup_op(
        name=p1_orch_fuzzy_dedup_name, prefix="p5_", params=args, host=orch_host, input_folder=doc_id.output
    )
    _set_component(fuzzy_dedup, "fuzzy dedup", doc_id)

    # proglang_select
    proglang_select = run_proglang_select_op(
        name=p1_orch_proglang_select_name,
        prefix="p6_",
        params=args,
        host=orch_host,
        input_folder=fuzzy_dedup.output,
    )
    _set_component(proglang_select, "proglang_select", fuzzy_dedup)

    # code_quality
    code_quality = run_code_quality_op(
        name=p1_orch_code_quality_name, prefix="p7_", params=args, host=orch_host, input_folder=proglang_select.output
    )
    _set_component(code_quality, "code_quality", proglang_select)

    # malware
    malware = run_malware_op(
        name=p1_orch_malware_name, prefix="p8_", params=args, host=orch_host, input_folder=code_quality.output
    )
    _set_component(malware, "malware", code_quality)
    # malware
    tokenization = run_tokenization_op(
        name=p1_orch_tokenization_wf_name, prefix="p9_", params=args, host=orch_host, input_folder=malware.output
    )
    _set_component(tokenization, "tokenization", malware)

    # Configure the pipeline level to one week (in seconds)
    dsl.get_pipeline_conf().set_timeout(ONE_WEEK_SEC)


if __name__ == "__main__":
    # Compiling the pipeline
    compiler.Compiler().compile(sample_code_ray_orchestrator, __file__.replace(".py", ".yaml"))
