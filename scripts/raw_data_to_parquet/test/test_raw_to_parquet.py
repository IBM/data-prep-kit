import io
import zipfile
from raw_data_to_parquet import zip_to_table, raw_to_parquet, generate_stats
from unittest.mock import patch, MagicMock
from parameterized import parameterized
import unittest
import pyarrow as pa


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

    @parameterized.expand(
        [
            (
                ["file1.txt", "file2.csv"],
                ["content of file1", "content of file2"],
                [
                    "file_path",
                    "document",
                    "contents",
                    "document_id",
                    "ext",
                    "hash",
                    "size",
                    "date_acquired",
                ],
            ),
        ]
    )
    def test_valid_zip(self, file_names, contents, expected_column_names):

        test_file_path = "test.zip"
        self.mock_instance.get_file.return_value = create_test_zip(file_names, contents)

        table = zip_to_table(self.mock_instance, test_file_path)

        # Assertions
        self.assertTrue(table)
        self.assertListEqual(table.column_names, expected_column_names)
        self.assertEqual(table.schema.field("file_path").type, pa.string())
        self.assertEqual(table.schema.field("document").type, pa.string())
        self.assertEqual(table.schema.field("contents").type, pa.string())
        self.assertEqual(table.schema.field("document_id").type, pa.string())
        self.assertEqual(table.schema.field("ext").type, pa.string())
        self.assertEqual(table.schema.field("hash").type, pa.string())
        self.assertEqual(table.schema.field("size").type, pa.int64())
        self.assertEqual(table.schema.field("date_acquired").type, pa.string())
        self.assertEqual(len(table), len(file_names))

        with patch(
            "data_processing.utils.TransformUtils.decode_content"
        ) as mock_decode_content:
            mock_decode_content.side_effect = Exception("Decoding failed")
            table = zip_to_table(self.mock_instance, test_file_path)
            self.assertEqual(len(table), 0)

    @parameterized.expand([(["empty_file.txt"], [""]), ([], [])])
    def test_empty_zip(self, file_names, contents):
        test_file_path = "test.zip"
        self.mock_instance.get_file.return_value = create_test_zip(file_names, contents)

        table = zip_to_table(self.mock_instance, test_file_path)

        self.assertFalse(table)


class TestRawToParquet(unittest.TestCase):

    def setUp(self):
        self.mock_data_access_factory_patcher = patch(
            "data_processing.data_access.DataAccessFactory"
        )
        self.mock_data_access_factory = self.mock_data_access_factory_patcher.start()
        self.mock_data_access_factory_instance = (
            self.mock_data_access_factory.return_value
        )
        self.addCleanup(self.mock_data_access_factory_patcher.stop)

        self.mock_data_access_instance = MagicMock()
        self.mock_data_access_factory_instance.create_data_access.return_value = (
            self.mock_data_access_instance
        )

    def test_process_zip_sucessfully(self):
        with patch("raw_data_to_parquet.zip_to_table") as mock_zip_to_table:
            mock_zip_to_table.return_value = "mocked_table"
            self.mock_data_access_instance.save_table.return_value = (0, {"k","v"})
            self.mock_data_access_instance.get_output_location.return_value = (
                "output_file.parquet"
            )

            result = raw_to_parquet(self.mock_data_access_factory_instance, "test.zip")

            # Assertions
            self.assertEqual(
                result, (True, {"path": "test.zip", "bytes_in_memory": 0})
            )
            self.mock_data_access_instance.get_output_location.assert_called_once_with(
                "test.zip"
            )
            self.mock_data_access_instance.save_table.assert_called_once_with(
                "output_file.parquet", "mocked_table"
            )

    def test_unsupported_file_type(self):
        result = raw_to_parquet(self.mock_data_access_factory, "test.txt")

        self.assertEqual(
            result,
            (False, {"path": "test.txt", "error": "Got .txt file, not supported"}),
        )

    def test_failed_to_upload(self):
        with patch("raw_data_to_parquet.zip_to_table") as mock_zip_to_table:
            mock_zip_to_table.return_value = "mocked_table"
            self.mock_data_access_instance.save_table.return_value = (0, {})

            result = raw_to_parquet(self.mock_data_access_factory_instance, "test.zip")

            # Assertions
            self.assertEqual(
                result, (False, {"path": "test.zip", "error": "Failed to upload"})
            )

    def test_exception_handling(self):
        with patch("raw_data_to_parquet.zip_to_table") as mock_zip_to_table:
            mock_zip_to_table.return_value = "mocked_table"
            self.mock_data_access_instance.save_table.side_effect = Exception(
                "Test exception"
            )

            result = raw_to_parquet(self.mock_data_access_factory_instance, "test.zip")

            self.assertEqual(
                result, (False, {"path": "test.zip", "error": "Test exception"})
            )


class TestGenerateStats(unittest.TestCase):
    def test_successful_metadata(self):
        metadata = [
            (True, {"path": "file1.txt", "bytes_in_memory": 100}),
            (True, {"path": "file2.txt", "bytes_in_memory": 200}),
        ]
        expected_result = {
            "total_files_given": 2,
            "total_files_processed": 2,
            "total_files_failed_to_processed": 0,
            "total_bytes_in_memory": 300,
            "failure_details": [],
        }
        result = generate_stats(metadata)

        self.assertEqual(result, expected_result)

    def test_failed_metadata(self):
        metadata = [
            (True, {"path": "file1.txt", "bytes_in_memory": 100}),
            (False, {"path": "file2.txt", "error": "Failed to process"}),
        ]
        expected_result = {
            "total_files_given": 2,
            "total_files_processed": 1,
            "total_files_failed_to_processed": 1,
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
            "total_bytes_in_memory": 0,
            "failure_details": [
                {"path": "file1.txt", "error": "Failed to process"},
                {"path": "file2.txt", "error": "Failed to process"},
            ],
        }
        result = generate_stats(metadata)
        
        self.assertEqual(result, expected_result)
