import os
import sys
from pathlib import Path

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from ededup_transform import EdedupTableTransformConfiguration


# create launcher
launcher = TransformLauncher(transform_runtime_config=EdedupTableTransformConfiguration())
# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": -1,
    "local_config": ParamsUtils.convert_to_ast(local_conf),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 3,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    "hash_cpu": 0.5,
    "num_hashes": 2,
    "doc_column": "contents",
}
sys.argv = ParamsUtils.dict_to_req(d=params)
Path(output_folder).mkdir(parents=True, exist_ok=True)

# launch
launcher.launch()
