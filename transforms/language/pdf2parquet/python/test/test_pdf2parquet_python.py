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

import ast
import os

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from pdf2parquet_transform_python import Pdf2ParquetPythonTransformConfiguration


class TestPythonPdf2ParquetTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        config = {
            "data_files_to_use": ast.literal_eval("['.pdf','.zip']"),
            "pdf2parquet_double_precision": 0,
        }

        # this is added as a fixture to remove these columns from comparison
        ignore_columns = ["date_acquired", "document_id", "pdf_convert_time"]

        fixtures = []
        launcher = PythonTransformLauncher(Pdf2ParquetPythonTransformConfiguration())

        # Default parameters
        fixtures.append(
            (
                launcher,
                {
                    **config,
                },
                basedir + "/input",
                basedir + "/expected",
                ignore_columns,
            )
        )

        # No table model and no OCR
        fixtures.append(
            (
                launcher,
                {
                    **config,
                    "pdf2parquet_contents_type": "text/markdown",
                    "pdf2parquet_do_ocr": False,
                    "pdf2parquet_do_table_structure": False,
                },
                basedir + "/input",
                basedir + "/expected_md_no_table_no_ocr",
                ignore_columns,
            )
        )

        # Produce JSON output
        fixtures.append(
            (
                launcher,
                {
                    **config,
                    "pdf2parquet_contents_type": "application/json",
                },
                basedir + "/input",
                basedir + "/expected_json",
                ignore_columns,
            )
        )

        return fixtures
