from abc import abstractmethod
from typing import Tuple, Union

import pyarrow as pa
from data_processing.transform import AbstractTableTransform


class AbstractTransformTest:
    """
    The test class for all/most AbstractTransform implementations.
    Generic tests are provided here, and sub-classes must implement the _get*_fixture() method(s)
    to provide the test data for a given test method.  For example,  get_test_transform_fixtures()
    provides the test data for the test_transform() test method.
    NOTE: Every sub-class must define the following in the .py file declaring their extending class as follows:
        def pytest_generate_tests(metafunc):
            test_instance = metafunc.cls()              # Create the instance of the class being tested.
            test_instance.install_fixtures(metafunc)    # Use it to install the fixtures
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
        test_instance.__install_fixtures(metafunc)  # Use it to install the fixtures

    def __install_fixtures(self, metafunc):
        # Apply the fixtures for the method with these input names (i.e. test_transform()).
        if (
            "transform" in metafunc.fixturenames
            and "in_table_list" in metafunc.fixturenames
            and "expected_table_list" in metafunc.fixturenames
            and "expected_metadata_list" in metafunc.fixturenames
        ):
            # Let the sub-class define the specific tests and test data for the transform under test.
            f = self.get_test_transform_fixtures()
            # Install the fixture, matching the parameter names used by test_transform() method.
            metafunc.parametrize("transform,in_table_list,expected_table_list,expected_metadata_list", f)

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
            l1 = len(t1)
            l2 = len(t2)
            assert l1 == l2, f"Number of rows in table #{i} ({l1}) does not match expected number ({l2})"
            for j in range(l1):
                r1 = t1[j]
                r2 = t2[j]
                assert r1 == r2, f"Row {j} of table {i} are not equal\n\tTransformed: {r1}\n\tExpected   : {2}"

    def validate_expected_metadata_lists(metadata: list[dict[str, float]], expected_metadata: list[dict[str, float]]):
        elen = len(expected_metadata)
        assert len(metadata) == elen, f"Number of metadata dictionaries not the expected of {elen}"
        for index in range(elen):
            AbstractTransformTest.validate_expected_metadata(metadata[index], expected_metadata[index])

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
        l1 = len(metadata)
        l2 = len(expected_metadata)
        assert metadata == expected_metadata, (
            f"Metadata not equal\n" "\tTransformed: {metadata}  Expected   : {expected_metadata}"
        )

    def test_transform(
        self,
        transform: AbstractTableTransform,
        in_table_list: list[pa.Table],
        expected_table_list: list[pa.Table],
        expected_metadata_list: list[dict[str, float]],
    ):
        """
        Use the given transform to transform() the given table(s) and compare the results (list of tables and metadata)
        with the expected values as given.  The inputs are provided by the sub-class definition of
        get_test_transform_fixtures().
        :param transform: transform to test.
        :param in_table_list:  table(s) to transform
        :param expected_table_list: the expected accumulation of output tables produced by the transform() call.
            This should include any empty tables if some of the calls to tranform() generate empty tables.
            If the final call to flush() produces an empty list of tables, these will not be included here (duh!).
            However, see expected_metadata_list for the handling of metadata produced by flush().
        :param expected_metadata_list: the expected list of accumulated metadata dictionaries across all calls to
            transform() and the final call to flush().  Transforms that produce nothing from flush() should include
            and empty dictionary at the end of this list.
        :return:
        """
        all_table_list = []
        all_metadata_list = []
        for in_table in in_table_list:
            table_list, metadata = transform.transform(in_table)
        all_table_list.extend(table_list)
        all_metadata_list.append(metadata)

        table_list, metadata = transform.flush()
        all_table_list.extend(table_list)
        all_metadata_list.append(metadata)

        AbstractTransformTest.validate_expected_tables(all_table_list, expected_table_list)
        AbstractTransformTest.validate_expected_metadata_lists(all_metadata_list, expected_metadata_list)

    def get_test_transform_fixtures(self) -> list[Tuple]:
        """
        Get the test data for the test_transform() test.
        :return:  a list of Tuples, to test. Each tuple contains the test inputs for test_transform() method.
            Item 0: The AbstractTableTransform to be tested
            Item 1: The input table to be transformed
            Item 2: The expected list of output tables for transformation of the input.
            Item 3: the expected metadata for transformation of the input.
        """
        raise NotImplemented()
