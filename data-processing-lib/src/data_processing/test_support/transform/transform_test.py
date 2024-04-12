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

from typing import Tuple

import pyarrow as pa
from data_processing.test_support.abstract_test import AbstractTest
from data_processing.transform import AbstractTableTransform


class AbstractTransformTest(AbstractTest):
    """
    The test class for all/most AbstractTransform implementations.
    Generic tests are provided here, and sub-classes must implement the _get*_fixture() method(s)
    to provide the test data for a given test method.  For example,  get_test_transform_fixtures()
    provides the test data for the test_transform() test method.
    """

    def _install_test_fixtures(self, metafunc):
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
