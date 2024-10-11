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

from code2parquet_transform import (  # domain_key,; snapshot_key,
    CodeToParquetTransform,
    data_factory_key,
    detect_programming_lang_key,
    domain_key,
    snapshot_key,
    supported_langs_file_key,
)
from data_processing.data_access import DataAccessFactory
from data_processing.test_support import get_files_in_folder
from data_processing.test_support.transform import AbstractBinaryTransformTest
from data_processing.utils import TransformUtils


class TestIngestToParquetTransform(AbstractBinaryTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        lang_supported_file = os.path.abspath(os.path.join(basedir, "languages/lang_extensions.json"))
        input_dir = os.path.join(basedir, "input")
        input_files = get_files_in_folder(input_dir, ".zip")
        input_files = [(name, binary) for name, binary in input_files.items()]
        expected_metadata_list = [{"number of rows": 2}, {"number of rows": 20}, {"number of rows": 52}, {}]
        config = {
            supported_langs_file_key: lang_supported_file,
            detect_programming_lang_key: True,
            data_factory_key: DataAccessFactory(),
        }

        expected_files = get_files_in_folder(os.path.join(basedir, "expected"), ".parquet")
        expected_files = [
            (binary, TransformUtils.get_file_extension(name)[1]) for name, binary in expected_files.items()
        ]

        return [(CodeToParquetTransform(config), input_files, expected_files, expected_metadata_list)]


if __name__ == "__main__":
    t = TestIngestToParquetTransform()
