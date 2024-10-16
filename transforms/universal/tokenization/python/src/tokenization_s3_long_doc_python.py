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

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from tokenization_transform_python import TokenizationPythonConfiguration


# create parameters
s3_cred = {
    "access_key": "localminioaccesskey",
    "secret_key": "localminiosecretkey",
    "url": "http://localhost:9000",
}
s3_conf = {
    "input_folder": "test/tokenization/ds02/input",
    "output_folder": "test/tokenization/ds02/output",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    # tokenization parameters
    "tkn_tokenizer": "hf-internal-testing/llama-tokenizer",
    "tkn_chunk_size": 20_000,
}
if __name__ == "__main__":
    """
    This illustrates the impact of `tkn_chunk_size` in tokenizing a lengthy document (~16.8 million characters).
    Tokenizing the entire document in one operation (i.e., with tkn_chunk_size=0) may
    require significantly more time compared to dividing it into chunks of approximately 20,000 characters each.
    """

    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = PythonTransformLauncher(TokenizationPythonConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
