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
    "access_key": "ba9b25bd8ca746cf9c7d77dc5bd02f13",
    "secret_key": "19bd3e7bdbd809859284d03513f49546f80660b39f15f038",
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

s3_conf = {
    "input_folder": "cos-optimal-llm-pile/sanity-test/input/dataset=big/",
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
    # resize config
    "resize_max_mbytes_per_table": 1,
    #    "resize_max_rows_per_table": 150
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
