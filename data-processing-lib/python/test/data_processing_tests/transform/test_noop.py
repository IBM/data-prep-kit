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
from data_processing.test_support.transform.noop_transform import NOOPPythonTransformConfiguration, sleep_cli_param

class TestPythonNOOPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        src_file_dir = os.path.abspath(os.path.dirname(__file__))
        fixtures = []
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               "../../../../../transforms/universal/noop/python/test-data"))
        launcher = PythonTransformLauncher(NOOPPythonTransformConfiguration())
        transform_config = {sleep_cli_param: 0}
        fixtures.append(
            (
                launcher,
                transform_config,
                basedir + "/input",
                basedir + "/expected",
                [],  # optional list of column names to ignore in comparing test-generated with expected.
            )
        )

        return fixtures
