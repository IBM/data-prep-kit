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
from doc_quality_transform_python import DocQualityPythonTransformConfiguration
from perplexity_transform_model import TransformerModel



class TestPythonDocQualityTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        model_path=os.path.join(basedir, "models")
        if not os.path.exists(model_path):
            model_path = os.path.abspath(os.path.join(basedir, "..", "models"))

        cli_params = {
            "docq_text_lang": "en",
            "docq_doc_content_column": "contents",
            "docq_bad_word_filepath": os.path.join(basedir, "ldnoobw", "en"),
            "docq_model_path": model_path,
            "docq_model_class_name": "TransformerModel",
            "docq_perplex_score_digit": 3,
        }
        basedir = os.path.abspath(os.path.join(basedir, "test-data"))
        fixtures = []
        launcher = PythonTransformLauncher(DocQualityPythonTransformConfiguration())
        fixtures.append((launcher, cli_params, os.path.join(basedir, "input"), os.path.join(basedir, "expected")))
        return fixtures
