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

from data_processing.test_support import get_files_in_folder
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from profiler_transform_python import ProfilerPythonTransformRuntimeConfiguration
from profiler_transform_base import doc_column_name_cli_param


class TestPythonProfilerTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        launcher = PythonTransformLauncher(ProfilerPythonTransformRuntimeConfiguration())
        config = {doc_column_name_cli_param: "contents"}
        return [(launcher, config, basedir + "/input", basedir + "/expected")]

    def _validate_directory_contents_match(self, produced: str, expected: str, ignore_columns: list[str] = []):
        # TODO add checking file content
        # Compare files
        f_set1 = list(get_files_in_folder(dir=produced, ext=".csv", return_data=False).keys())
        f_set2 = list(get_files_in_folder(dir=expected, ext=".csv", return_data=False).keys())
        assert len(f_set1) == len(f_set2)
        produced_len = 0
        expected_len = 0
        for i in range(len(f_set1)):
            produced_len += os.path.getsize(f_set1[i])
            expected_len += os.path.getsize(f_set2[i])
        assert abs(produced_len - expected_len) < 500

        # Compare metadata
        f_set1 = get_files_in_folder(dir=produced, ext=".json", return_data=False)
        f_set2 = get_files_in_folder(dir=expected, ext=".json", return_data=False)
        assert len(f_set1) == len(f_set2)

