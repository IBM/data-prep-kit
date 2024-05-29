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

import io
import unittest
import zipfile
from unittest.mock import MagicMock, patch

import pyarrow as pa
from ingest2parquet import generate_stats, raw_to_parquet, zip_to_table
from parameterized import parameterized
from utils import detect_language


def create_test_zip(file_names, contents_list):
    """
    Generates an in-memory ZIP file for testing.

    params:
        file_names (list): List of filenames within the ZIP.
        contents_list (list): List of corresponding content strings.

    returns:
        bytes: The ZIP file contents as a byte array.
    """

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w") as zip_file:
        for filename, content in zip(file_names, contents_list):
            zip_file.writestr(filename, content)
    return zip_buffer.getvalue()


class TestZipToTable(unittest.TestCase):
    def setUp(self):
        self.mock_data_access_patcher = patch("data_processing.data_access.DataAccess")
        self.mock_data_access = self.mock_data_access_patcher.start()
        self.mock_instance = self.mock_data_access.return_value
        self.addCleanup(self.mock_data_access_patcher.stop)

        self.mock_detect_lang_patcher = patch("utils.detect_langauge.Detect_Programming_Lang")
        self.mock_detect_lang = self.mock_data_access_patcher.start()
        self.mock_detect_lang_instance = self.mock_detect_lang.return_value
        self.addCleanup(self.mock_detect_lang_patcher.stop)

    @parameterized.expand(
        [
            (
                ["file1.txt", "file2.csv"],
                ["content of file1", "content of file2"],
                [
                    "title",
                    "document",
                    "contents",
                    "document_id",
                    "ext",
                    "hash",
                    "size",
                    "date_acquired",
                    "programming_language",
                ],
            ),
        ]
    )
    def test_valid_zip(self, file_names, contents, expected_column_names):
        test_file_path = "test.zip"
        self.mock_instance.get_file.return_value = create_test_zip(file_names, contents)
        self.mock_detect_lang_instance.get_lang_from_ext.return_value = "unknown"

        table = zip_to_table(self.mock_instance, test_file_path, self.mock_detect_lang_instance)

        # Assertions
        self.assertTrue(table)
        self.assertListEqual(table.column_names, expected_column_names)
        self.assertEqual(table.schema.field("title").type, pa.string())
        self.assertEqual(table.schema.field("document").type, pa.string())
        self.assertEqual(table.schema.field("contents").type, pa.string())
        self.assertEqual(table.schema.field("document_id").type, pa.string())
        self.assertEqual(table.schema.field("ext").type, pa.string())
        self.assertEqual(table.schema.field("hash").type, pa.string())
        self.assertEqual(table.schema.field("size").type, pa.int64())
        self.assertEqual(table.schema.field("date_acquired").type, pa.string())
        self.assertEqual(table.schema.field("programming_language").type, pa.string())
        self.assertEqual(len(table), len(file_names))

        with patch("data_processing.utils.TransformUtils.decode_content") as mock_decode_content:
            mock_decode_content.side_effect = Exception("Decoding failed")
            table = zip_to_table(self.mock_instance, test_file_path, True)
            self.assertEqual(len(table), 0)

    @parameterized.expand([(["empty_file.txt"], [""]), ([], [])])
    def test_empty_zip(self, file_names, contents):
        test_file_path = "test.zip"
        self.mock_instance.get_file.return_value = create_test_zip(file_names, contents)

        table = zip_to_table(self.mock_instance, test_file_path, True)
        self.assertEqual(table.num_rows, 0)


class TestRawToParquet(unittest.TestCase):
    def setUp(self):
        self.mock_data_access_factory_patcher = patch("data_processing.data_access.DataAccessFactory")
        self.mock_data_access_factory = self.mock_data_access_factory_patcher.start()
        self.mock_data_access_factory_instance = self.mock_data_access_factory.return_value
        self.addCleanup(self.mock_data_access_factory_patcher.stop)

        self.mock_data_access_instance = MagicMock()
        self.mock_data_access_factory_instance.create_data_access.return_value = self.mock_data_access_instance

    def test_process_zip_sucessfully(self):
        with patch("data_processing.utils.TransformUtils.add_column") as mock_add_column:
            with patch("ingest2parquet.zip_to_table") as mock_zip_to_table:
                mock_zip_to_table.return_value = pa.Table.from_pylist(
                    [
                        {
                            "title": "file1.txt",
                            "document": "test.zip",
                            "contents": "content of file1",
                            "document_id": "297d7c07-8c00-4a1b-9235-555cb3a9ddd9",
                            "ext": ".txt",
                            "hash": "56ff3012a6bce1711715608340ebed7a2765fc4493354141b9a96259beeb1d68",
                            "size": 65,
                            "date_acquired": "2024-04-24T12:44:18.930550",
                            "programming_language": "unknown",
                        }
                    ]
                )
                table = pa.Table.from_pylist(
                    [
                        {
                            "title": "file1.txt",
                            "document": "test.zip",
                            "contents": "content of file1",
                            "document_id": "297d7c07-8c00-4a1b-9235-555cb3a9ddd9",
                            "ext": ".txt",
                            "hash": "56ff3012a6bce1711715608340ebed7a2765fc4493354141b9a96259beeb1d68",
                            "size": 65,
                            "date_acquired": "2024-04-24T12:44:18.930550",
                            "programming_language": "unknown",
                            "domain": "code",
                            "snapshot": "github",
                        }
                    ]
                )
                mock_add_column.return_value = table
                self.mock_data_access_instance.save_table.return_value = (0, {"k", "v"})
                self.mock_data_access_instance.get_output_location.return_value = "output_file.parquet"

                result = raw_to_parquet(self.mock_data_access_factory_instance, "test.zip", True, "github", "code")

                # Assertions
                self.assertEqual(
                    result,
                    (True, {"path": "test.zip", "bytes_in_memory": 0, "row_count": 1}),
                )
                self.mock_data_access_instance.get_output_location.assert_called_once_with("test.zip")
                self.mock_data_access_instance.save_table.assert_called_once_with("output_file.parquet", table)

    def test_unsupported_file_type(self):
        result = raw_to_parquet(self.mock_data_access_factory, "test.txt", True, "github", "code")

        self.assertEqual(
            result,
            (False, {"path": "test.txt", "error": "Got .txt file, not supported"}),
        )

    def test_failed_to_upload(self):
        with patch("ingest2parquet.zip_to_table") as mock_zip_to_table:
            mock_zip_to_table.return_value = pa.Table.from_pylist([])
            self.mock_data_access_instance.save_table.return_value = (0, {})

            result = raw_to_parquet(self.mock_data_access_factory_instance, "test.zip", True, "github", "code")

            # Assertions
            self.assertEqual(result, None)

    def test_exception_handling(self):
        with patch("ingest2parquet.zip_to_table") as mock_zip_to_table:
            table = pa.Table.from_pylist(
                [
                    {
                        "title": "file1.txt",
                        "document": "test.zip",
                        "contents": "content of file1",
                        "document_id": "297d7c07-8c00-4a1b-9235-555cb3a9ddd9",
                        "ext": ".txt",
                        "hash": "56ff3012a6bce1711715608340ebed7a2765fc4493354141b9a96259beeb1d68",
                        "size": 65,
                        "date_acquired": "2024-04-24T12:44:18.930550",
                        "programming_language": "unknown",
                        "domain": "code",
                        "snapshot": "github",
                    }
                ]
            )
            mock_zip_to_table.return_value = table
            self.mock_data_access_instance.save_table.side_effect = Exception("Test exception")

            result = raw_to_parquet(self.mock_data_access_factory_instance, "test.zip", True, "githib", "NL")

            self.assertEqual(result, (False, {"path": "test.zip", "error": "Test exception"}))


class TestGenerateStats(unittest.TestCase):
    def test_successful_metadata(self):
        metadata = [
            (True, {"path": "file1.txt", "bytes_in_memory": 100, "row_count": 10}),
            (True, {"path": "file2.txt", "bytes_in_memory": 200, "row_count": 20}),
        ]
        expected_result = {
            "total_files_given": 2,
            "total_files_processed": 2,
            "total_files_failed_to_processed": 0,
            "total_no_of_rows": 30,
            "total_bytes_in_memory": 300,
            "failure_details": [],
        }
        result = generate_stats(metadata)

        self.assertEqual(result, expected_result)

    def test_failed_metadata(self):
        metadata = [
            (True, {"path": "file1.txt", "bytes_in_memory": 100, "row_count": 10}),
            (False, {"path": "file2.txt", "error": "Failed to process"}),
        ]
        expected_result = {
            "total_files_given": 2,
            "total_files_processed": 1,
            "total_files_failed_to_processed": 1,
            "total_no_of_rows": 10,
            "total_bytes_in_memory": 100,
            "failure_details": [{"path": "file2.txt", "error": "Failed to process"}],
        }
        result = generate_stats(metadata)

        self.assertEqual(result, expected_result)

    def test_empty_metadata(self):
        metadata = []
        expected_result = {
            "total_files_given": 0,
            "total_files_processed": 0,
            "total_files_failed_to_processed": 0,
            "total_no_of_rows": 0,
            "total_bytes_in_memory": 0,
            "failure_details": [],
        }
        result = generate_stats(metadata)

        self.assertEqual(result, expected_result)

    def test_no_successful_entries(self):
        metadata = [
            (False, {"path": "file1.txt", "error": "Failed to process"}),
            (False, {"path": "file2.txt", "error": "Failed to process"}),
        ]
        expected_result = {
            "total_files_given": 2,
            "total_files_processed": 0,
            "total_files_failed_to_processed": 2,
            "total_no_of_rows": 0,
            "total_bytes_in_memory": 0,
            "failure_details": [
                {"path": "file1.txt", "error": "Failed to process"},
                {"path": "file2.txt", "error": "Failed to process"},
            ],
        }
        result = generate_stats(metadata)

        self.assertEqual(result, expected_result)


class TestDetectProgrammingLang(unittest.TestCase):
    def setUp(self):
        self.detector = detect_language.Detect_Programming_Lang()

    def test_get_lang_from_ext_known(self):
        ext = ".py"
        lang = self.detector.get_lang_from_ext(ext)
        self.assertEqual(lang, "Python")

    def test_get_lang_from_ext_unknown(self):
        ext = ".xyz"
        lang = self.detector.get_lang_from_ext(ext)
        self.assertEqual(lang, "unknown")
