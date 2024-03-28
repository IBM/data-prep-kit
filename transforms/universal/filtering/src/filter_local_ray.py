import json
import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils

from transforms.universal.filtering.src.filter_transform import (
    FilterTransformConfiguration,
    sql_params_dict_cli_param,
    sql_statement_cli_param,
)


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}

filter_sql_statement = "SELECT * FROM input_table WHERE title='https://poker'"
filter_sql_params_dict = {}

filter_params = {
    sql_statement_cli_param: filter_sql_statement,
    sql_params_dict_cli_param: json.dumps(filter_sql_params_dict),
}

worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
launcher_params = {
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
}

# launch
if __name__ == "__main__":
    # Run the transform inside Ray
    # Create the CLI args as will be parsed by the launcher
    sys.argv = ParamsUtils.dict_to_req(launcher_params | filter_params)
    # Create the longer to launch with the blocklist transform.
    launcher = TransformLauncher(transform_runtime_config=FilterTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
