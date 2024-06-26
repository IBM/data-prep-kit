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
from lang_id_transform_python import LangIdentificationPythonTransformConfiguration
from lang_models import KIND_FASTTEXT


class TestPythonLangIdentificationTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli_params = {
            "lang_id_model_credential": "PUT YOUR OWN HUGGINGFACE CREDENTIAL",
            "lang_id_model_kind": KIND_FASTTEXT,
            "lang_id_model_url": "facebook/fasttext-language-identification",
            "lang_id_content_column_name": "text",
            "lang_id_output_lang_column_name": "ft_lang",
            "lang_id_output_score_column_name": "ft_score",
        }
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        fixtures = []
        launcher = PythonTransformLauncher(LangIdentificationPythonTransformConfiguration())
        fixtures.append((launcher, cli_params, basedir + "/input", basedir + "/expected"))
        return fixtures
