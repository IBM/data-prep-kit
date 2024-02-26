from abc import abstractmethod
from typing import Tuple, Union

import pyarrow as pa
from data_processing.transform import AbstractTableTransform


def _validate_expected_tables(table_list: list[pa.Table], expected_table_list: list[pa.Table]):
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
            assert r1 == r2, f"Row {j} of table {i} are not equal\n" "\tTransformed: " + r1 + "\tExpected   : " + r2


def _validate_expected_metadata(metadata: dict[str, float], expected_metadata: dict[str, float]):
    assert isinstance(metadata, dict), f"Did not generate metadata of type dict"
    assert isinstance(expected_metadata, dict), f"Test misconfigured, expected metadata is not a dictionary"
    l1 = len(metadata)
    l2 = len(expected_metadata)
    assert metadata == expected_metadata, (
        f"Metadata at not equal\n" "\tTransformed: " + metadata + "\tExpected   : " + expected_metadata
    )


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

    def install_fixtures(self, metafunc):
        # Apply the fixtures for the method with these input names (i.e. test_transform()).
        if (
            "transform" in metafunc.fixturenames
            and "in_table" in metafunc.fixturenames
            and "expected_table_list" in metafunc.fixturenames
            and "expected_metadata" in metafunc.fixturenames
        ):
            # Let the sub-class define the specific tests and test data for the transform under test.
            f = self.get_test_transform_fixtures()
            # Install the fixture, matching the parameter names used by test_transform() method.
            metafunc.parametrize("transform,in_table,expected_table_list,expected_metadata", f)

    def test_transform(
        self,
        transform: AbstractTableTransform,
        in_table: pa.Table,
        expected_table_list: list[pa.Table],
        expected_metadata: dict[str, float],
    ):
        """
        Use the given transform to transform() the given table and compare the results (list of tables and metadata)
        with the expected values as given.  The inputs are provided by the sub-class definition of
        get_test_transform_fixtures().
        :param transform: transform to test.
        :param in_table:  table to transform
        :param expected_table_list:
        :param expected_metadata:
        :return:
        """
        table_list, metadata = transform.transform(in_table)
        _validate_expected_tables(table_list, expected_table_list)
        _validate_expected_metadata(metadata, expected_metadata)
        table_list, metadata = transform.flush()
        assert (
            table_list is not None
        ), f"Flushing a simple transform returned None for the list of tables. Expected an empty list."
        assert len(table_list) == 0, f"Flushing a simple transform returned a list of non-zero length"
        assert (
            metadata is not None
        ), f"Flushing a simple transform returned None for metadata. Expected an empty dictionary."
        assert len(metadata) == 0, f"Flushing a simple transform returned a dictionary of non-zero length"

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
