import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import DPFConfig, ParamsUtils
from fdedup_transform import FdedupTableTransformConfiguration


# create launcher
launcher = TransformLauncher(transform_runtime_config=FdedupTableTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": DPFConfig.S3_ACCESS_KEY,
    "secret_key": DPFConfig.S3_SECRET_KEY,
    "cos_url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "cos-optimal-llm-pile/sanity-test/input/dataset=fuzzy_dedup/",
    "output_folder": "cos-optimal-llm-pile/boris-da-test/",
    # "input_folder": "cos-optimal-llm-pile/test/david/input/",
    # "output_folder": "cos-optimal-llm-pile/test/david/output/",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": -1,
    "s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "s3_config": ParamsUtils.convert_to_ast(s3_conf),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 3,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # columns used
    "doc_column": "contents",
    "id_column": "int_id_column",
    "cluster_column": "cluster",
    # infrastructure
    "bucket_cpu": 0.5,
    "doc_cpu": 0.5,
    "mhash_cpu": 0.5,
    "num_doc_actors": 1,
    "num_bucket_actors": 1,
    "num_minhash_actors": 1,
    "num_preprocessors": 2,
    # fuzzy parameters
    "num_permutations": 64,
    "threshold": 0.8,
    "shingles_size": 5,
    "japanese_data": False,
    "delimiters": " ",
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
