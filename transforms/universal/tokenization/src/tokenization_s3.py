import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from tokenization_transform import TokenizationTransformConfiguration


print(os.environ)
# create launcher
launcher = TransformLauncher(transform_runtime_config=TokenizationTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": os.environ.get("COS_ACCESS_KEY", "access"),
    "secret_key": os.environ.get("COS_SECRET_KEY", "secret"),
    "cos_url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "cos-optimal-llm-pile/sanity-test/input/dataset=text/",
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
    "num_workers": 5,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    "Tokenization_sleep_sec": 5,
}
sys.argv = ParamsUtils.dict_to_req(d=params)
# for arg in sys.argv:
#     print(arg)

# launch
launcher.launch()
