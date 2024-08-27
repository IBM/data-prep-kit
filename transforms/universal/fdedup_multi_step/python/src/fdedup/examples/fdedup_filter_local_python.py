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

import os
import sys

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from fdedup.transforms.base import (filter_doc_column_name_cli_param,
                                    filter_int_column_name_cli_param,
                                    filter_cluster_column_name_cli_param,
                                    filter_removed_docs_column_name_cli_param,
                                    filter_doc_id_snapshot_directory_cli_param,
                                    )
from fdedup.transforms.python import FdedupFilterPythonTransformRuntimeConfiguration

# create launcher
launcher = PythonTransformLauncher(FdedupFilterPythonTransformRuntimeConfiguration())
# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # orchestrator
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # fdedup parameters
    filter_doc_column_name_cli_param: "contents",
    filter_int_column_name_cli_param: "Unnamed: 0",
    filter_cluster_column_name_cli_param: "cluster",
    filter_removed_docs_column_name_cli_param: "removed",
    filter_doc_id_snapshot_directory_cli_param: os.path.join(input_folder, "snapshot/docs"),
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
