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

import ast
import os
import sys
from pathlib import Path

from data_processing.utils import ParamsUtils
from ingest2parquet import ingest2parquet


if __name__ == "__main__":
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
    output_folder = os.path.join(os.path.join(os.path.dirname(__file__), "..", "test-data"), "output")
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    params = {
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        "data_files_to_use": ast.literal_eval("['.zip']"),
        "detect_programming_lang": True,
        "snapshot": "github",
        "domain": "code",
    }

    sys.argv = ParamsUtils.dict_to_req(d=params)
    ingest2parquet()
