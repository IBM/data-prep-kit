import os
import sys
from pathlib import Path

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from antivirus_transform import AntivirusTransformConfiguration


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "output"))
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
    "num_workers": 5,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    "input_column": "contents",
    "output_column": "virus_detection",
    "network_socket": "localhost:3310",
}
if __name__ == "__main__":
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    launcher = TransformLauncher(transform_runtime_config=AntivirusTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
