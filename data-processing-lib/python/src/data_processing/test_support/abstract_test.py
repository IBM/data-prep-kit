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
import shutil
from abc import abstractmethod
from filecmp import dircmp

import pyarrow as pa
from data_processing.data_access import DataAccessLocal
from data_processing.utils import TransformUtils, get_logger


logger = get_logger(__name__)


def get_tables_in_folder(dir: str) -> list[pa.Table]:
    """
    Get  list of Tables loaded from the parquet files in the given directory.  The returned
    list is sorted lexigraphically by the name of the file.
    :param dir:
    :return:
    """
    dal = DataAccessLocal()
    files, _ = dal.get_folder_files(dir, extensions=[".parquet"])
    return [TransformUtils.convert_binary_to_arrow(data) for data in files.values()]


def get_files_in_folder(dir: str, ext: str, return_data: bool = True) -> dict[str, bytes]:
    """
    Get  list of files in folder. Always return file names and conditionally their content
    :param dir: directory
    :param ext: extension
    :param return_data: flag to return file's content - default True
    :return:
    """
    dal = DataAccessLocal()
    files, _ = dal.get_folder_files(dir, extensions=[ext], return_data=return_data)
    return files


class AbstractTest:
    """
    This class enables a test convention for pytest in which sub-classes define the tests and a method to install
    the pytest test fixtures for the tests.  This class's primary responsibility is to use the pytest hook
    method pytest_generate_tests() to have the sub-class install the fixtures using _install_test_fixtures().
    Some generic static test methods are also provided here.
    """

    @staticmethod
    def pytest_generate_tests(metafunc):
        """
        Called by pytest to install the fixtures for the test class in this file.
        This method name (i.e. pytest_generate_tests) must not be changed, otherwise the fixtures
        will not be installed.
        :param metafunc:
        :return:
        """
        test_instance = metafunc.cls()  # Create the instance of the test class being used.
        test_instance._install_test_fixtures(metafunc)  # Use it to install the fixtures

    @abstractmethod
    def _install_test_fixtures(self, metafunc):
        raise NotImplemented("Sub-class must implemented this to install the fixtures for its tests.")

    @staticmethod
    def validate_expected_tables(table_list: list[pa.Table], expected_table_list: list[pa.Table]):
        """
        Verify with assertion messages that the two lists of Tables are equivalent.
        :param table_list:
        :param expected_table_list:
        :return:
        """
        assert table_list is not None, "Transform output table is None"
        assert expected_table_list is not None, "Test misconfigured: expected table list is None"

        l1 = len(table_list)
        l2 = len(expected_table_list)
        assert l1 == l2, f"Number of transformed tables ({l1}) is not the expected number ({l2})"
        for i in range(l1):
            t1 = table_list[i]
            t2 = expected_table_list[i]
            assert t1.schema == t2.schema, f"Schema of the two tables is not the same"
            rows1 = t1.num_rows
            rows2 = t2.num_rows
            assert rows1 == rows2, f"Number of rows in table #{i} ({rows1}) does not match expected number ({rows2})"
            for j in range(rows1):
                r1 = t1.take([j])
                r2 = t2.take([j])
                assert r1 == r2, f"Row {j} of table {i} are not equal\n\tTransformed: {r1}\n\tExpected   : {r2}"

    @staticmethod
    def validate_expected_files(files_list: list[tuple[bytes, str]], expected_files_list: list[tuple[bytes, str]]):
        """
        Verify with assertion messages that the two lists of Tables are equivalent.
        :param files_list:
        :param expected_files_list:
        :return:
        """
        assert files_list is not None, "Transform output table is None"
        assert expected_files_list is not None, "Test misconfigured: expected table list is None"

        l1 = len(files_list)
        l2 = len(expected_files_list)
        assert l1 == l2, f"Number of transformed files ({l1}) is not the expected number ({l2})"
        for i in range(l1):
            f1 = files_list[i]
            f2 = expected_files_list[i]
            assert (
                f1[1] == f2[1]
            ), f"produced file extension {f1[1]} is different from  expected file extension {f2[1]}"
            assert (
                len(f1[0]) - len(f2[0])
            ) < 50, f"produced file length {len(f1[0])} is different from expected file extension {len(f2[0])}"

    @staticmethod
    def validate_expected_metadata_lists(metadata: list[dict[str, float]], expected_metadata: list[dict[str, float]]):
        elen = len(expected_metadata)
        assert len(metadata) == elen, f"Number of metadata dictionaries not the expected of {elen}"
        for index in range(elen):
            AbstractTest.validate_expected_metadata(metadata[index], expected_metadata[index])

    @staticmethod
    def validate_expected_metadata(metadata: dict[str, float], expected_metadata: dict[str, float]):
        """
        Verify with assertion messages that the two dictionaries are as expected.
        :param metadata:
        :param expected_metadata:
        :return:
        """
        assert metadata is not None, "Transform output metadata is None"
        assert expected_metadata is not None, "Test misconfigured: expected metadata is None"
        assert isinstance(metadata, dict), f"Did not generate metadata of type dict"
        assert isinstance(expected_metadata, dict), f"Test misconfigured, expected metadata is not a dictionary"
        assert metadata == expected_metadata, (
            f"Metadata not equal\n" "\tTransformed: {metadata}  Expected   : {expected_metadata}"
        )

    @staticmethod
    def validate_directory_contents(directory: str, expected_dir: str, drop_columns: list[str] = []):
        """
        Make sure the directory contents are the same.
        :param directory:
        :param expected_dir:
        :param drop_columns: list of columns that might differ
        :return:
        """
        dir_cmp = dircmp(directory, expected_dir, ignore=[".DS_Store"])
        len_funny = len(dir_cmp.funny_files)  # Do this earlier than an assert for debugging reasons.
        assert len_funny == 0, f"Files that could compare, but couldn't be read for some reason: {dir_cmp.funny_files}"
        assert len(dir_cmp.common_funny) == 0, f"Types of the following files don't match: {dir_cmp.common_funny}"
        assert len(dir_cmp.right_only) == 0, f"Files found only in expected output directory: {dir_cmp.right_only}"
        assert len(dir_cmp.left_only) == 0, f"Files missing in test output directory: {dir_cmp.left_only}"
        if "metadata.json" in dir_cmp.diff_files:
            # metadata.json has things like dates and times and output folders.
            logger.warning("Differences in metadata.json being ignored for now.")
            expected_diffs = 1
        else:
            expected_diffs = 0
        failed = len(dir_cmp.diff_files) != expected_diffs
        if failed:
            AbstractTest.__confirm_diffs(directory, expected_dir, dir_cmp.diff_files, "/tmp", drop_columns)

        # Traverse into the subdirs since dircmp doesn't seem to do that.
        subdirs = [f.name for f in os.scandir(expected_dir) if f.is_dir()]
        for subdir in subdirs:
            d1 = os.path.join(directory, subdir)
            d2 = os.path.join(expected_dir, subdir)
            AbstractTest.validate_directory_contents(d1, d2, drop_columns)

    @staticmethod
    def _validate_table_files(parquet1: str, parquet2: str, drop_columns: list[str] = []):
        da = DataAccessLocal()
        t1, _ = da.get_table(parquet1)
        t2, _ = da.get_table(parquet2)
        if len(drop_columns) > 0:
            t1 = t1.drop_columns(drop_columns)
            t2 = t2.drop_columns(drop_columns)
        AbstractTest.validate_expected_tables([t1], [t2])

    @staticmethod
    def __confirm_diffs(
        src_dir: str, expected_dir: str, diff_files: list, dest_dir: str, drop_columns: list[str] = []
    ):
        """
        Copy all files from the source dir to the dest dir.
        :param src_dir:
        :param expected_dir:
        :param diff_files:
        :param dest_dir:
        :return:
        """
        for file in diff_files:
            expected = os.path.join(expected_dir, file)
            src = os.path.join(src_dir, file)
            dest = os.path.join(dest_dir, file)
            if "parquet" in file:
                # It seems file can be different on disk, but contain the same column/row values.
                # so for these, do the inmemory comparison.
                try:
                    AbstractTest._validate_table_files(expected, src, drop_columns)
                except AssertionError as e:
                    logger.info(f"Copying file with difference: {src} to {dest}")
                    shutil.copyfile(src, dest)
                    raise e
            elif "metadata" in file:
                pass
            else:
                # These are binary files.
                da = DataAccessLocal()
                f1_bytes = da.get_file(src)
                f2_bytes = da.get_file(dest)
                assert (
                    len(f1_bytes) - len(f2_bytes)
                ) < 50, (
                    f"produced file length {len(f1_bytes)} is different from expected file extension {len(f2_bytes)}"
                )
