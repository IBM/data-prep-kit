from abc import abstractmethod
from filecmp import dircmp

import pyarrow as pa
from data_processing.utils import get_logger


logger = get_logger(__name__)


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
            l1 = t1.num_rows
            l2 = t2.num_rows
            assert l1 == l2, f"Number of rows in table #{i} ({l1}) does not match expected number ({l2})"
            for j in range(l1):
                r1 = t1.take([j])[0]
                r2 = t2.take([j])[0]
                assert r1 == r2, f"Row {j} of table {i} are not equal\n\tTransformed: {r1}\n\tExpected   : {2}"

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
    def validate_directory_contents(directory: str, expected_dir: str):
        """
        Make sure the directory contents are the same.
        :param directory:
        :param expected_dir:
        :return:
        """
        dir_cmp = dircmp(directory, expected_dir)
        assert len(dir_cmp.funny_files) == 0, \
            f"Files that could compare, but couldn't be read for some reason: {dir_cmp.funny_files}"
        assert len(dir_cmp.common_funny) == 0, f"Types of the following files don't match: {dir_cmp.common_funny}"
        assert len(dir_cmp.right_only) == 0, f"Files found only in expected output directory: {dir_cmp.right_only}"
        assert len(dir_cmp.left_only) == 0, f"Files files missing in test output directory: {dir_cmp.left_only}"
        if "metadata.json" in dir_cmp.diff_files:
            # metadata.json has things like dates and times and output folders.
            logger.warning("Differences in metadata.json being ignored for now.")
            assert len(dir_cmp.diff_files) == 1, f"Files that did not match the expected {dir_cmp.diff_files}"
        else:
            assert len(dir_cmp.diff_files) == 0, f"Files that did not match the expected {dir_cmp.diff_files}"
