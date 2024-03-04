import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from lang_id_implementation import (
    PARAM_CONTENT_COLUMN_NAME,
    PARAM_MODEL_KIND,
    PARAM_MODEL_PATH,
    LangIdentificationTableTransformConfiguration,
)
from lang_models import KIND_FASTTEXT


print(os.environ)
# create launcher
launcher = TransformLauncher(transform_runtime_config=LangIdentificationTableTransformConfiguration())
# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data"))
output_folder = os.path.abspath(os.path.join(input_folder, "output"))

# Configure local folders
local_conf = {"input_folder": input_folder, "output_folder": output_folder}

worker_options = {"num_cpus": 1}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": -1,
    "local_config": ParamsUtils.convert_to_ast(local_conf),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 2,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    PARAM_MODEL_KIND: KIND_FASTTEXT,
    # PARAM_MODEL_PATH: "PATH TO YOUR MODEL",
    PARAM_MODEL_PATH: "/root/lid.176.ftz",
    PARAM_CONTENT_COLUMN_NAME: "text",
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
