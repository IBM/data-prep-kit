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

import gzip
import json
import os
from pathlib import Path
from unittest.mock import patch

import pyarrow
import pytest
from data_processing.data_access import DataAccessLocal
from data_processing.utils import GB, MB, get_logger


logger = get_logger(__name__)


class TestInit:
    path_dict = {
        "input_folder": os.path.join(os.sep, "tmp", "input_guf"),
        "output_folder": os.path.join(os.sep, "tmp", "output_guf"),
    }
    dal = DataAccessLocal(path_dict, d_sets=["dset1", "dset2"], checkpoint=True, m_files=-1)
    size_stat_dict_empty = {"max_file_size": 0.0, "min_file_size": float(GB), "total_file_size": 0.0}
    size_stat_dict = {"max_file_size": 0.0, "min_file_size": 0.0, "total_file_size": 0.0}
    size_stat_dict_1 = {"max_file_size": 1.0, "min_file_size": 0.0, "total_file_size": 1.0}
    size_stat_dict_1_1 = {"max_file_size": 1.0, "min_file_size": 1.0, "total_file_size": 1.0}


class TestGetFilesFolder(TestInit):
    def test_empty_directory(self):
        """
        Tests the function with an empty directory.
        """
        directory = os.path.join(self.dal.input_folder, "empty_dir")
        os.makedirs(directory, exist_ok=True)
        result = self.dal._get_files_folder(directory, cm_files=0)
        os.rmdir(directory)
        assert result == ([], self.size_stat_dict_empty)

    def test_single_file(self):
        """
        Tests the function with a single parquet file.
        """
        directory = Path(os.path.join(self.dal.input_folder, "empty_dir"))
        file_path = directory / "file.parquet"
        os.makedirs(directory, exist_ok=True)
        file_path.touch()
        result = self.dal._get_files_folder(str(directory), cm_files=0)
        os.remove(file_path)
        os.rmdir(directory)
        assert result == (["/tmp/input_guf/empty_dir/file.parquet"], self.size_stat_dict)

    def test_multiple_files(self):
        """
        Tests the function with multiple parquet files.
        """
        directory = Path(os.path.join(self.dal.input_folder, "empty_dir"))
        files = [directory / f"file{i}.parquet" for i in range(3)]
        os.makedirs(directory, exist_ok=True)
        for file in files:
            file.touch()
        with open(files[0], "w") as fp:
            fp.write(" " * MB)

        file_list, size_dict = self.dal._get_files_folder(str(directory), cm_files=0)
        results = (file_list, size_dict)
        expected_results = ([str(file.absolute()) for file in files], self.size_stat_dict_1)
        for file in files:
            os.remove(file)
        os.rmdir(directory)
        assert results == expected_results

    def test_non_parquet_file(self):
        """
        Tests the function with a non-parquet file.
        """
        directory = Path(os.path.join(self.dal.input_folder, "empty_dir"))
        file_path = directory / "file.txt"
        os.makedirs(directory, exist_ok=True)
        file_path.touch()
        result = self.dal._get_files_folder(str(directory), cm_files=0)
        os.remove(file_path)
        os.rmdir(directory)
        assert result == ([], self.size_stat_dict_empty)

    def test_non_existent_directory(self):
        """
        Tests the function with a non-existent directory.
        """
        directory = Path("non_existent_dir")
        with patch("pathlib.Path.rglob") as mock_rglob:
            mock_rglob.return_value = []
            result = self.dal._get_files_folder(str(directory), cm_files=0)
        assert result == ([], self.size_stat_dict_empty)


class TestGetInputFiles(TestInit):
    def setup_directories(self, prefix=""):
        """
        Utility function that sets up input and output directories
        """
        #        input_path = Path(os.path.join(self.dal.input_folder, f"{prefix}input_dir"))
        #        output_path = Path(os.path.join(self.dal.output_folder, f"{prefix}output_dir"))
        input_path = Path(self.dal.input_folder)
        output_path = Path(self.dal.output_folder)
        os.makedirs(input_path, exist_ok=True)
        os.makedirs(output_path, exist_ok=True)
        return input_path, output_path

    @staticmethod
    def cleanup(directories_to_remove=[], files_to_remove=[]):
        """
        Utility function that removes all the files and all the directories
        passed as arguments
        """
        for file_to_remove in files_to_remove:
            try:
                os.remove(file_to_remove)
            except Exception as ex:
                print(f"Failed to remove file {file_to_remove}: {ex}")
        for directory_to_remove in directories_to_remove:
            try:
                os.rmdir(directory_to_remove)
            except Exception as ex:
                print(f"Failed to remove directory {directory_to_remove}: {ex}")

    def test_empty_directories(self):
        """
        Test the function with empty input and output directories.
        """
        input_path, output_path = self.setup_directories("empty_")
        result = self.dal._get_input_files(str(input_path), str(output_path), cm_files=0)
        self.cleanup(directories_to_remove=[input_path, output_path])
        assert result == ([], self.size_stat_dict_empty)

    def test_missing_files(self):
        """
        Test the function with one file missing in the output directory.
        """
        input_path, output_path = self.setup_directories()
        input_file = input_path / "file1.parquet"
        output_file = output_path / "file2.parquet"
        input_file.touch()
        output_file.touch()
        result = self.dal._get_input_files(str(input_path), str(output_path), cm_files=0)
        self.cleanup(
            directories_to_remove=[input_path, output_path],
            files_to_remove=[input_file, output_file],
        )
        assert result == ([str(input_file.absolute())], self.size_stat_dict)

    def test_multiple_missing_files(self):
        """
        Test the function with multiple files missing in the output directory.
        """
        input_path, output_path = self.setup_directories()
        input_files = [input_path / f"file{i}.parquet" for i in range(3)]
        output_file = output_path / "file2.parquet"
        for file in input_files:
            file.touch()
        output_file.touch()

        file_list, size_dict = self.dal._get_input_files(str(input_path), str(output_path), cm_files=0)
        result = (file_list, size_dict)
        expected_result = (
            [str(file.absolute()) for file in input_files if str(file.name) != str(output_file.name)],
            self.size_stat_dict,
        )

        input_files.append(output_file)
        self.cleanup(
            directories_to_remove=[input_path, output_path],
            files_to_remove=input_files,
        )
        assert result == expected_result

    def test_non_existent_directory(self):
        """
        Test the function with a non-existent input directory.
        """
        nonexistent_input_path = Path("non_existent_dir")
        input_path, output_path = self.setup_directories()
        with patch("pathlib.Path.rglob") as mock_rglob:
            mock_rglob.return_value = []
            result = self.dal._get_input_files(str(nonexistent_input_path), str(output_path), cm_files=0)
            self.cleanup(directories_to_remove=[input_path, output_path])
            assert result == ([], self.size_stat_dict_empty)

    def test_non_parquet_files(self):
        """
        Test the function with non-parquet files in the input directory.
        """
        input_path, output_path = self.setup_directories()
        input_file = input_path / "file.txt"
        input_file.touch()
        result = self.dal._get_input_files(str(input_path), str(output_path), cm_files=0)
        self.cleanup(
            directories_to_remove=[input_path, output_path],
            files_to_remove=[input_file],
        )
        assert result == ([], self.size_stat_dict_empty)


class TestGetFilesToProcess(TestInit):
    def setup_directories(self, dset=""):
        """
        Utility function that sets up input and output directories
        """
        input_path = Path(os.path.join(self.dal.input_folder, dset))
        output_path = Path(os.path.join(self.dal.output_folder, dset))
        os.makedirs(input_path, exist_ok=True)
        os.makedirs(output_path, exist_ok=True)
        return input_path, output_path

    @staticmethod
    def cleanup(directories_to_remove=[], files_to_remove=[]):
        """
        Utility function that removes all the files and all the directories
        passed as arguments
        """
        for file_to_remove in files_to_remove:
            try:
                os.remove(file_to_remove)
            except Exception as ex:
                print(f"Failed to remove file {file_to_remove}: {ex}")
        for directory_to_remove in directories_to_remove:
            try:
                os.rmdir(directory_to_remove)
            except Exception as ex:
                print(f"Failed to remove directory {directory_to_remove}: {ex}")

    def test_empty_directories(self):
        """
        Test the function with empty input and output directories and
        no data set sub-directories.
        """
        self.cleanup(directories_to_remove=[self.dal.input_folder, self.dal.output_folder])
        os.makedirs(self.dal.input_folder, exist_ok=True)
        os.makedirs(self.dal.output_folder, exist_ok=True)
        original_d_sets = self.dal.d_sets
        self.dal.d_sets = None
        result = self.dal.get_files_to_process()
        self.dal.d_sets = original_d_sets
        assert result == ([], self.size_stat_dict_empty)

    def test_missing_files(self):
        """
        Test the function with one file missing in the output directory.
        """
        # create the data set sub-directories
        in_path_1, out_path_1 = self.setup_directories(dset="dset1")
        in_path_2, out_path_2 = self.setup_directories(dset="dset2")

        in_file_1 = in_path_1 / "file1.parquet"
        in_file_1.touch()
        result = self.dal.get_files_to_process()
        print(f"result {result}")
        self.cleanup(
            directories_to_remove=[in_path_1, out_path_1, in_path_2, out_path_2],
            files_to_remove=[in_file_1],
        )
        assert result == ([str(in_file_1.absolute())], self.size_stat_dict)

    def multiple_missing_files_setup(self):
        # create the data set sub-directories
        in_path_1, out_path_1 = self.setup_directories(dset="dset1")
        in_path_2, out_path_2 = self.setup_directories(dset="dset2")
        in_files_1 = [in_path_1 / f"file{i}.parquet" for i in range(3)]
        in_files_2 = [in_path_2 / f"file{i}.parquet" for i in range(2)]
        out_file_2 = out_path_2 / "file1.parquet"
        for file in in_files_1:
            file.touch()
        for file in in_files_2:
            file.touch()
        out_file_2.touch()
        with open(in_files_1[0], "w") as fp:
            fp.write(" " * MB)
        file_list, size_dict = self.dal.get_files_to_process()
        result = (file_list, size_dict)
        return result, in_files_1, in_files_2, out_file_2, in_path_1, out_path_1, in_path_2, out_path_2

    def multiple_missing_files_cleanup(
        self, in_files_1, in_files_2, out_file_2, in_path_1, out_path_1, in_path_2, out_path_2
    ):
        files_to_remove = in_files_1 + in_files_2 + [out_file_2]
        directories_to_remove = [in_path_1, out_path_1, in_path_2, out_path_2]
        self.cleanup(directories_to_remove=directories_to_remove, files_to_remove=files_to_remove)

    def test_multiple_missing_files(self):
        """
        Test the function with multiple files missing in the output directory.
        """
        (
            result,
            in_files_1,
            in_files_2,
            out_file_2,
            in_path_1,
            out_path_1,
            in_path_2,
            out_path_2,
        ) = self.multiple_missing_files_setup()

        expected_result = (
            [str(file.absolute()) for file in in_files_1]
            + [str(file.absolute()) for file in in_files_2 if str(file.name) != str(out_file_2.name)],
            self.size_stat_dict_1,
        )
        self.multiple_missing_files_cleanup(
            in_files_1, in_files_2, out_file_2, in_path_1, out_path_1, in_path_2, out_path_2
        )

        assert result == expected_result

        self.dal.checkpoint = False
        (
            result,
            in_files_1,
            in_files_2,
            out_file_2,
            in_path_1,
            out_path_1,
            in_path_2,
            out_path_2,
        ) = self.multiple_missing_files_setup()
        expected_result = (
            [str(file.absolute()) for file in in_files_1] + [str(file.absolute()) for file in in_files_2],
            self.size_stat_dict_1,
        )
        self.multiple_missing_files_cleanup(
            in_files_1, in_files_2, out_file_2, in_path_1, out_path_1, in_path_2, out_path_2
        )
        assert result == expected_result

        self.dal.m_files = 1
        (
            result,
            in_files_1,
            in_files_2,
            out_file_2,
            in_path_1,
            out_path_1,
            in_path_2,
            out_path_2,
        ) = self.multiple_missing_files_setup()
        expected_result = ([str(in_files_1[0].absolute())], self.size_stat_dict_1_1)

        self.multiple_missing_files_cleanup(
            in_files_1, in_files_2, out_file_2, in_path_1, out_path_1, in_path_2, out_path_2
        )
        assert result == expected_result

    def test_non_existent_dataset(self):
        """
        Test the function with a non-existent input directory.
        """
        original_d_sets = self.dal.d_sets
        self.dal.d_sets = None
        with patch("pathlib.Path.rglob") as mock_rglob:
            mock_rglob.return_value = []
            result = self.dal.get_files_to_process()
            self.dal.d_sets = original_d_sets
            assert result == ([], self.size_stat_dict_empty)

    def test_non_parquet_files(self):
        """
        Test the function with non-parquet files in the input directory.
        """
        # create the data set sub-directories
        in_path_1, out_path_1 = self.setup_directories(dset="dset1")
        in_path_2, out_path_2 = self.setup_directories(dset="dset2")
        in_files_1 = [in_path_1 / f"file{i}.txt" for i in range(3)]
        in_files_2 = [in_path_2 / f"file{i}.txt" for i in range(2)]
        out_file_2 = out_path_2 / "file1.parquet"
        for file in in_files_1:
            file.touch()
        for file in in_files_2:
            file.touch()
        out_file_2.touch()
        result = self.dal.get_files_to_process()
        files_to_remove = in_files_1 + in_files_2 + [out_file_2]
        self.cleanup(
            directories_to_remove=[in_path_1, out_path_1, in_path_2, out_path_2],
            files_to_remove=files_to_remove,
        )
        assert result == ([], self.size_stat_dict_empty)


class TestReadPyarrowTable(TestInit):
    # Sample data for test table
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}

    # Create PyArrow schema
    schema = pyarrow.schema([pyarrow.field("col1", pyarrow.int32()), pyarrow.field("col2", pyarrow.string())])

    # Create PyArrow table
    table = pyarrow.Table.from_pydict(mapping=data, schema=schema)

    # Write the table to a parquet file
    pq_file_path = os.path.join(os.sep, "tmp", "test_file.parquet")
    pyarrow.parquet.write_table(table, pq_file_path)

    @pytest.mark.parametrize(
        "path, expected_error",
        [
            ("non_existent_file.parquet", FileNotFoundError),
            ("invalid_file.txt", IOError),
            ("malformed_parquet.parquet", pyarrow.ArrowException),
        ],
    )
    def test_error_handling(self, path, expected_error):
        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_read_table.side_effect = expected_error
            table = self.dal.get_table(path)
            assert table is None
            assert mock_read_table.called_once_with(path)

    def test_successful_read(self):
        table = self.dal.get_table(self.pq_file_path)
        os.remove(self.pq_file_path)
        assert isinstance(table, pyarrow.Table)


class TestGetOutputLocation(TestInit):
    def test_get_output_location(self):
        in_path = os.path.join(self.dal.input_folder, "path", "to", "f.parquet")
        out_path = self.dal.get_output_location(in_path)
        assert out_path == os.path.join(self.dal.output_folder, "path", "to", "f.parquet")


class TestSavePyarrowTable(TestInit):

    # Create a simple PyArrow table
    table = pyarrow.Table.from_pydict(mapping={"a": [1, 2], "b": ["string1", "string2"]})

    # path to save parquet tables
    pq_file_path = os.path.join(os.sep, "tmp", "output.parquet")

    def test_successful_save(self):

        # Mock os methods for testing
        with patch("os.path.getsize") as mock_getsize, patch("os.path.basename") as mock_basename:
            mock_getsize.return_value = 1024
            mock_basename.return_value = "test_file.parquet"
            size_in_memory, file_info = self.dal.save_table(self.pq_file_path, self.table)
            os.remove(self.pq_file_path)
            # Assertions about return values
            assert size_in_memory == self.table.nbytes
            assert file_info == {"name": "test_file.parquet", "size": 1024}

    def test_save_error(self):
        # Mock pq.write_table to raise an exception
        with patch("pyarrow.parquet.write_table") as mock_write_table:
            mock_write_table.side_effect = Exception("Write error")

            _, file_info = self.dal.save_table(self.pq_file_path, self.table)

            # Assertions about return values
            assert file_info is None


class TestSaveJobMetadata(TestInit):
    metadata = {
        "pipeline": "pipeline",
        "job details": "job details",
        "code": "code",
        "job_input_params": "job_input_params",
        "execution_stats": "execution_stats",
        "job_output_stats": "job_output_stats",
    }

    @staticmethod
    def cleanup(directories_to_remove=[], files_to_remove=[]):
        """
        Utility function that removes all the files and all the directories
        passed as arguments
        """
        for file_to_remove in files_to_remove:
            try:
                os.remove(file_to_remove)
            except Exception as ex:
                print(f"Failed to remove file {file_to_remove}: {ex}")
        for directory_to_remove in directories_to_remove:
            try:
                os.rmdir(directory_to_remove)
            except Exception as ex:
                print(f"Failed to remove directory {directory_to_remove}: {ex}")

    def test_save_job_metadata(self):
        file_info = self.dal.save_job_metadata(metadata=self.metadata)
        metadata_file_path = file_info.get("name", "")
        with open(metadata_file_path, "r") as fp:
            metadata_dict = json.load(fp)
        self.cleanup(
            directories_to_remove=[self.dal.input_folder, self.dal.output_folder], files_to_remove=[metadata_file_path]
        )
        assert metadata_file_path == os.path.join(self.dal.output_folder, "metadata.json")
        assert (
            len(metadata_dict) == 8
            and metadata_dict.get("source", {}).get("name", "") == self.dal.input_folder
            and metadata_dict.get("target", {}).get("name", "") == self.dal.output_folder
        )


class TestGetFile(TestInit):

    # create test files (text and gzip)
    text_file_path = os.path.join(os.sep, "tmp", "test_file.txt")
    gzip_file_path = os.path.join(os.sep, "tmp", "test_file.gz")

    with open(text_file_path, "wb") as f:
        f.write(b"This is a test file.")
    with gzip.open(gzip_file_path, "wb") as f:
        f.write(b"This is a compressed test file.")

    def test_existing_txt_file(self):
        data = self.dal.get_file(self.text_file_path)
        os.remove(self.text_file_path)
        assert data == b"This is a test file."

    def test_existing_gz_file(self):
        data = self.dal.get_file(self.gzip_file_path)
        os.remove(self.gzip_file_path)
        assert data == b"This is a compressed test file."

    def test_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            self.dal.get_file("nonexistent_file.txt")

    def test_invalid_gz_file(self):
        with open("invalid_gz.gz", "wb") as f:
            f.write(b"Invalid data")
        with pytest.raises(gzip.BadGzipFile):
            self.dal.get_file("invalid_gz.gz")
        os.remove("invalid_gz.gz")

    def test_mock_open(self):
        with patch("builtins.open") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = b"Mock data"
            data = self.dal.get_file("some_file.txt")
            assert data == b"Mock data"


class TestGetFolderFiles(TestInit):

    # create test folder and test files (text pdf and bin) inside test folder
    test_dir = os.path.join(os.sep, "tmp", "test_folder")
    os.makedirs(test_dir, exist_ok=True)
    test_domain_dir = os.path.join(test_dir, "domains")
    os.makedirs(test_domain_dir, exist_ok=True)
    text_file_path = os.path.join(test_dir, "test_file.txt")
    pdf_file_path = os.path.join(test_dir, "test_file.pdf")
    bin_file_path = os.path.join(test_dir, "invalid_bin_file.bin")
    domain_file_path = os.path.join(test_domain_dir, "domains")
    with open(text_file_path, "wb") as f:
        f.write(b"This is text content.")
    with open(pdf_file_path, "wb") as f:
        f.write(b"\x25\x50\x44\x46")  # Sample PDF header
    with open(bin_file_path, "wb") as f:
        f.write(b"Not a text file")
    with open(domain_file_path, "wb") as f:
        f.write(b"asdf.qwer.com")

    def test_one_extension(self):
        contents_txt = self.dal.get_folder_files(self.test_dir, [".txt"])
        assert len(contents_txt) == 1
        assert self.text_file_path in contents_txt
        assert contents_txt[self.text_file_path] == b"This is text content."

    def test_one_non_existing_extension(self):
        contents_jpg = self.dal.get_folder_files(self.test_dir, [".jpg"])
        assert not contents_jpg

    def test_all_files(self):
        contents_all = self.dal.get_folder_files(self.test_dir)
        assert len(contents_all) == 4
        assert contents_all[self.text_file_path] == b"This is text content."
        assert contents_all[self.pdf_file_path][:4] == b"\x25\x50\x44\x46"
        assert contents_all[self.bin_file_path] == b"Not a text file"
        assert contents_all[self.domain_file_path] == b"asdf.qwer.com"

    def test_multiple_extensions(self):
        contents_txt_pdf = self.dal.get_folder_files(self.test_dir, [".txt", ".pdf"])
        assert len(contents_txt_pdf) == 2
        assert contents_txt_pdf[self.text_file_path] == b"This is text content."
        assert contents_txt_pdf[self.pdf_file_path][:4] == b"\x25\x50\x44\x46"  # Check PDF header

    def test_nonexistent_files(self):
        contents = self.dal.get_folder_files("nonexistent_dir", ["txt"])
        os.remove(self.text_file_path)
        os.remove(self.pdf_file_path)
        os.remove(self.bin_file_path)
        os.remove(self.domain_file_path)
        os.rmdir(self.test_domain_dir)
        os.rmdir(self.test_dir)
        assert not contents


class TestSaveFile(TestInit):

    new_file_path = os.path.join(os.sep, "tmp", "new_file.bin")

    def test_successful_save(self):
        file_info = self.dal.save_file(self.new_file_path, b"This is new data")
        assert file_info == {"name": self.new_file_path, "size": os.path.getsize(self.new_file_path)}
        os.remove(self.new_file_path)

    def test_invalid_filename(self):
        file_info = self.dal.save_file("", b"Data")
        assert file_info is None
