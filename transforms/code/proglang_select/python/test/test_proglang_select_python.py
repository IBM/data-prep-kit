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
from proglang_select_transform import (
    lang_allowed_langs_file_key,
    lang_lang_column_key,
    lang_output_column_key,
)
from proglang_select_transform_python import ProgLangSelectPythonConfiguration


class TestPythonProgLangSelectTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        languages_file = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "../test-data/languages/allowed-code-languages.txt",
            )
        )
        config = {
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the lang_select_local_config configuration.
            lang_allowed_langs_file_key: languages_file,
            lang_lang_column_key: "language",
            lang_output_column_key: "allowed_languages",
        }
        fixtures = [
            (
                PythonTransformLauncher(ProgLangSelectPythonConfiguration()),
                config,
                basedir + "/input",
                basedir + "/expected",
            )
        ]
        return fixtures
