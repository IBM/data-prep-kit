import os
import sys
from pathlib import Path

from blocklist_transform import (
    BlockListTransformConfiguration,
    annotation_column_name_key,
    blocked_domain_list_path_key,
    source_url_column_name_key,
)
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils


# create parameters

blocklist_conf_url = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "domains"))
blocklist_annotation_column_name = "blocklisted"
blocklist_doc_source_url_column = "title"

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
    blocked_domain_list_path_key: blocklist_conf_url,
    annotation_column_name_key: blocklist_annotation_column_name,
    source_url_column_name_key: blocklist_doc_source_url_column,
}

# launch
if __name__ == "__main__":
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    launcher = TransformLauncher(transform_runtime_config=BlockListTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
