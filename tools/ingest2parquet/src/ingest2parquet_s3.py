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
import sys

from data_processing.utils import ParamsUtils
from ingest2parquet import ingest2parquet


# create parameters
s3_cred = {
    "access_key": "localminioaccesskey",
    "secret_key": "localminiosecretkey",
    "url": "http://localhost:9000",
}
s3_conf = {
    "input_folder": "test/ingest2parquet/input",
    "output_folder": "test/ingest2parquet/output",
}
params = {
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    "data_files_to_use": ast.literal_eval("['.zip']"),
    "detect_programming_lang": True,
    "snapshot": "github",
    "domain": "code",
}
sys.argv = ParamsUtils.dict_to_req(d=params)
if __name__ == "__main__":
    ingest2parquet()
