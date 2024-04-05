import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import DPFConfig, ParamsUtils
from doc_id_transform import DocIDTransformConfiguration


# create launcher
launcher = TransformLauncher(transform_runtime_config=DocIDTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": DPFConfig.S3_ACCESS_KEY,
    "secret_key": DPFConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "cos-optimal-llm-pile/sanity-test/input/dataset=text/",
    "output_folder": "cos-optimal-llm-pile/boris-da-test/",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 3,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # doc id
    "doc_id_doc_column": "contents",
    "doc_id_hash_column": "hash_column",
    "doc_id_int_column": "int_id_column",
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
