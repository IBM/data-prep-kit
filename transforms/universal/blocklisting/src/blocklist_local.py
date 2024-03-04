import os
import sys
from pathlib import Path


print(f"sys.path = {sys.path}")

from blocklist_transform import BlockListTransformConfiguration
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils


# create launcher
launcher = TransformLauncher(transform_runtime_config=BlockListTransformConfiguration())
# create parameters

blocklist_conf_url = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "domains"))
blocklist_column_name = "url_blocklisting_refinedweb"
blocklist_doc_source_url = "title"

input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
output_folder = os.path.join(input_folder, "output")
Path(output_folder).mkdir(parents=True, exist_ok=True)
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
    "blocklist_conf_url": blocklist_conf_url,
    "blocklist_column_name": blocklist_column_name,
    "blocklist_doc_source_url": blocklist_doc_source_url,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
