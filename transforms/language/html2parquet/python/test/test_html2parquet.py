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
import pyarrow as pa
from data_processing.data_access.data_access_local import DataAccessLocal
from data_processing.test_support import get_files_in_folder
from data_processing.test_support.transform import AbstractBinaryTransformTest
from data_processing.utils import TransformUtils
from html2parquet_transform import Html2ParquetTransform


class TestHtml2ParquetTransform(AbstractBinaryTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        dal = DataAccessLocal()
        basedir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../test-data")
        )
        input_dir = os.path.join(basedir, "input")
        input_files = get_files_in_folder(input_dir, ".html")
        input_files = [(name, binary) for name, binary in input_files.items()]
        expected_metadata_list = [{"nrows": 1}, {}]

        config = {}

        expected_files = [
            os.path.join(basedir, "expected", TransformUtils.get_file_basename(input_file).replace(".html", ".parquet"))
            for input_file, _ in input_files
        ]

        expected_files = [
            (dal.get_file(name)[0], TransformUtils.get_file_extension(name)[1])
            for name in expected_files
        ]
        return [
            (
                Html2ParquetTransform(config),
                input_files,
                expected_files,
                expected_metadata_list,
            )
        ]
