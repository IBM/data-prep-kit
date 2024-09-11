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

import pyarrow as pa
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.abstract_test import _allowed_float_percent_diff
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from docling_core.types import Document
from docling_core.types.doc.base import BaseText
from pdf2parquet_transform_python import Pdf2ParquetPythonTransformConfiguration
from pydantic import ValidationError


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
        ignore_columns = ["date_acquired", "document_id", "pdf_convert_time", "hash"]

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

    @classmethod
    def validate_expected_row(
        cls,
        table_index: int,
        row_index: int,
        test_row: pa.Table,
        expected_row: pa.Table,
    ):
        """
        Compare the two rows for equality, allowing float values to be within a percentage
        of each other as defined by global _allowed_float_percent_diff.
        We assume the schema has already been compared and is equivalent.
        Args:
            table_index: index of tables that is the source of the rows.
            row_index:
            test_row:
            expected_row:
        """

        assert test_row.num_rows == 1, "Invalid usage.  Expected test table with 1 row"
        assert (
            expected_row.num_rows == 1
        ), "Invalid usage.  Expected expected table with 1 row"
        if test_row != expected_row:
            # Else look for floating point values that might differ within the allowance
            msg = f"Row {row_index} of table {table_index} are not equal\n\tTransformed: {test_row}\n\tExpected   : {expected_row}"
            assert test_row.num_columns == expected_row.num_columns, msg
            num_columns = test_row.num_columns
            for i in range(num_columns):
                # Over each cell/column in the row
                test_column = test_row.column(i)
                expected_column = expected_row.column(i)
                if test_column != expected_column:
                    # Check if the value is a float and if so, allow a fuzzy match
                    test_value = test_column.to_pylist()[0]
                    expected_value = expected_column.to_pylist()[0]

                    # Test for Document type
                    try:

                        expected_doc = Document.model_validate_json(expected_value)
                        test_doc = Document.model_validate_json(test_value)
                        cls.validate_documents(
                            row_index=row_index,
                            table_index=table_index,
                            test_doc=test_doc,
                            expected_doc=expected_doc,
                        )

                        continue

                    except ValidationError:
                        pass

                    # Test for floats
                    is_float = isinstance(test_value, float) and isinstance(
                        expected_value, float
                    )
                    if is_float:
                        # It IS a float, so allow a fuzzy match
                        allowed_diff = abs(_allowed_float_percent_diff * expected_value)
                        diff = abs(test_value - expected_value)
                        assert diff <= allowed_diff, msg

                        continue

                    # Its NOT a float or other managed types, so do a normal compare
                    assert test_column == expected_column, msg

    @classmethod
    def validate_documents(
        cls,
        row_index: int,
        table_index: int,
        test_doc: Document,
        expected_doc: Document,
    ):

        msg = f"Row {row_index} of table {table_index} are not equal\n\t"
        assert(test_doc.main_text is not None, msg + "Test document has no content")
        assert len(test_doc.main_text) == len(expected_doc.main_text), (
            msg + f"Main Text lengths do not match."
        )

        for i in range(len(expected_doc.main_text)):
            expected_item = expected_doc.main_text[i]
            test_item = test_doc.main_text[i]

            # Validate type
            assert expected_item.obj_type == test_item.obj_type, (
                msg + f"Object type does not match."
            )

            # Validate text content
            if isinstance(expected_item, BaseText):
                assert expected_item.text == test_item.text, (
                    msg + f"Text does not match."
                )
