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

from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher
from lang_id_transform import (
    content_column_name_cli_param,
    model_credential_cli_param,
    model_kind_cli_param,
    model_url_cli_param,
    output_lang_column_name_cli_param,
    output_score_column_name_cli_param,
)
from lang_id_transform_ray import LangIdentificationRayTransformConfiguration
from lang_models import KIND_FASTTEXT


class TestRayLangIdentificationTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        config = {
            model_credential_cli_param: "PUT YOUR OWN HUGGINGFACE CREDENTIAL",
            model_kind_cli_param: KIND_FASTTEXT,
            model_url_cli_param: "facebook/fasttext-language-identification",
            content_column_name_cli_param: "text",
            output_lang_column_name_cli_param: "ft_lang",
            output_score_column_name_cli_param: "ft_score",
            "run_locally": True,
        }
        return [
            (
                RayTransformLauncher(LangIdentificationRayTransformConfiguration()),
                config,
                basedir + "/input",
                basedir + "/expected",
            )
        ]
