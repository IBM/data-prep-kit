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

from data_processing.runtime import AbstractTransformLauncher
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from license_copyright_removal_transform import (
    LicenseCopyrightRemovalPythonTransformConfiguration,
    column_cli_params,
    license_cli_params,
    copyright_cli_params,
)


class AbstractPythonLicenseCopyrightRemovalTransformTest(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def _get_launcher(self) -> (AbstractTransformLauncher, dict):
        """
        Allow other runtimes to override with a different Launcher but share the test fixtures.
        Returns: the launcher and any additional command line/configuration included in the
        list of args given as the 2nd element of the fixtures.
        """
        return (PythonTransformLauncher(LicenseCopyrightRemovalPythonTransformConfiguration()), {})

    def _get_test_file_directory(self) -> str:
        raise NotImplemented

    def get_test_transform_fixtures(self) -> list[tuple]:
        fixtures = []
        basedir = self._get_test_file_directory()
        basedir = os.path.abspath(os.path.join(basedir, "../test-data"))

        launcher, args = self._get_launcher()
        fixtures.append(
            (
                launcher,
                args
                | {
                    column_cli_params: 'contents',
                    license_cli_params: 'true',
                    copyright_cli_params: 'true',
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "license-and-copyright"),
            )
        )

        launcher, args = self._get_launcher()
        fixtures.append(
            (
                launcher,
                args
                | {
                    column_cli_params: 'contents',
                    license_cli_params: 'false',
                    copyright_cli_params: 'true',
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "copyright"),
            )
        )

        launcher, args = self._get_launcher()
        fixtures.append(
            (
                launcher,
                args
                | {
                    column_cli_params: 'contents',
                    license_cli_params: 'true',
                    copyright_cli_params: 'false',
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "license"),
            )
        )
        return fixtures
