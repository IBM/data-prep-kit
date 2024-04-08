import os
import sys
from pathlib import Path

from antivirus_local import check_clamd
from antivirus_transform import AntivirusTransformConfiguration
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils


TEST_SOCKET = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".tmp", "clamd.ctl"))
# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
antivirus_params = {
    "antivirus_input_column": "contents",
    "antivirus_output_column": "virus_detection",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 5,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
}
if __name__ == "__main__":
    check_clamd()
    # Here we show to run the transform in the ray launcher
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params | antivirus_params)
    # create launcher
    launcher = TransformLauncher(transform_runtime_config=AntivirusTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
