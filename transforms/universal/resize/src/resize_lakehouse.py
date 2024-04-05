import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import DPFConfig, ParamsUtils
from resize_transform import ResizeTransformConfiguration


print(os.environ)
# create launcher
launcher = TransformLauncher(transform_runtime_config=ResizeTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": DPFConfig.S3_ACCESS_KEY,
    "secret_key": DPFConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "academic.ieee",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "academic.ieee_splitfile_test",
    "output_path": "lh-test/tables/academic/ieee_splitfile_test",
    "token": DPFConfig.LAKEHOUSE_TOKEN,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_lh_config": ParamsUtils.convert_to_ast(lakehouse_config),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 1,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # "resize_max_mbytes_per_table": 0.02,
    "resize_max_rows_per_table": 75,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
