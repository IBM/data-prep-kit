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
from get_duplicate_list_transform import sort_output_cli_param
from get_duplicate_list_transform_python import (
    GetDuplicateListPythonTransformConfiguration,
)


class TestPythonGetDuplicateListTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            sort_output_cli_param: True,
        }
        launcher = PythonTransformLauncher(GetDuplicateListPythonTransformConfiguration())
        fixtures = [
            (
                launcher,
                config,
                os.path.join(basedir, "expected", "cluster_analysis"),
                os.path.join(basedir, "expected", "get_list_transform"),
            )
        ]
        return fixtures
