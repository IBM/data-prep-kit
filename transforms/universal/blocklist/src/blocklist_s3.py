# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import sys

from blocklist_transform import (
    BlockListTransformConfiguration,
    annotation_column_name_cli_param,
    blocked_domain_list_path_cli_param,
    source_url_column_name_cli_param,
)
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils


# create parameters
s3_cred = {
    "access_key": "localminioaccesskey",
    "secret_key": "localminiosecretkey",
    "url": "http://localhost:9000",
}
s3_conf = {
    "input_folder": "test/blocklist/input",
    "output_folder": "test/blocklist/output",
}

blocklist_annotation_column_name = "blocklisted"
blocklist_doc_source_url_column = "title"

block_list_params = {
    blocked_domain_list_path_cli_param: "test/blocklist/domains",
    "blocklist_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    annotation_column_name_cli_param: blocklist_annotation_column_name,
    source_url_column_name_cli_param: blocklist_doc_source_url_column,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
launcher_params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 5,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
}

# launch
if __name__ == "__main__":
    # Run the transform inside Ray
    # Create the CLI args as will be parsed by the launcher
    sys.argv = ParamsUtils.dict_to_req(launcher_params | block_list_params)
    # Create the longer to launch with the blocklist transform.
    launcher = TransformLauncher(transform_runtime_config=BlockListTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
