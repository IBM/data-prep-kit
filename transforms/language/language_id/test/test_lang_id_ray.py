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

# This helps to be able to run the test from within an IDE which seems to use the location of the
# file as the working directory.

import os

import pyarrow as pa
from data_processing.test_support.ray import AbstractTransformLauncherTest
from lang_id_transform import (
    PARAM_CONTENT_COLUMN_NAME,
    PARAM_MODEL_CREDENTIAL,
    PARAM_MODEL_KIND,
    PARAM_MODEL_URL,
    LangIdentificationTableTransformConfiguration,
)
from lang_models import KIND_FASTTEXT


class TestLanguageIdentificationTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = [
            (
                LangIdentificationTableTransformConfiguration(),
                {
                    PARAM_MODEL_KIND: KIND_FASTTEXT,
                    PARAM_MODEL_CREDENTIAL: "YOUR HUGGING FACE ACCOUNT TOKEN",
                    PARAM_MODEL_URL: "facebook/fasttext-language-identification",
                    PARAM_CONTENT_COLUMN_NAME: "text",
                },
                basedir + "/input",
                basedir + "/expected",
            )
        ]
        return fixtures
