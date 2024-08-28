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
from fdedup.transforms.base import (preprocessor_doc_column_name_cli_param,
                                    preprocessor_int_column_name_cli_param,
                                    delimiters_cli_param,
                                    preprocessor_num_permutations_cli_param,
                                    preprocessor_threshold_cli_param,
                                    shingles_size_cli_param,
                                    preprocessor_minhash_snapshot_directory_cli_param)
from fdedup.transforms.python import FdedupPreprocessorPythonTransformRuntimeConfiguration


class TestPythonFdedupPreprocessorTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        # The following based on 3 identical input files of about 39kbytes, and 200 rows
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test-data"))
        launcher = PythonTransformLauncher(FdedupPreprocessorPythonTransformRuntimeConfiguration())
        config = {preprocessor_doc_column_name_cli_param: "contents",
                  preprocessor_int_column_name_cli_param: "Unnamed: 0",
                  delimiters_cli_param: " ",
                  preprocessor_num_permutations_cli_param: 64,
                  preprocessor_threshold_cli_param: .8,
                  shingles_size_cli_param: 5,
                  }
        return [(launcher, config, basedir + "/input", basedir + "/preprocessor")]
