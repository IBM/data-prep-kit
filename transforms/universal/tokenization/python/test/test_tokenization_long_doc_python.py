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

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from tokenization_transform_python import TokenizationPythonConfiguration


tkn_params = {
    "tkn_tokenizer": "hf-internal-testing/llama-tokenizer",
    "tkn_doc_id_column": "document_id",
    "tkn_doc_content_column": "contents",
    "tkn_text_lang": "en",
    "tkn_chunk_size": 20_000,
}


class TestPythonTokenizationTransform(AbstractTransformLauncherTest):
    """
    This involves testing a tokenizer by tokenizing a lengthy document
    through segmenting it into chunks and then tokenizing each segment individually.
    By tokenizing the text in this manner and subsequently merging the tokenized chunks,
    it contributes to enhancing the overall runtime efficiency.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        launcher = PythonTransformLauncher(TokenizationPythonConfiguration())
        fixtures = [(launcher, tkn_params, basedir + "/ds02/input", basedir + "/ds02/expected")]
        return fixtures
