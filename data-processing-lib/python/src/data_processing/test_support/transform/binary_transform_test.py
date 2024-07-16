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

from data_processing.test_support.abstract_test import AbstractTest
from data_processing.transform import AbstractBinaryTransform


class AbstractBinaryTransformTest(AbstractTest):
    """
    The test class for all/most AbstractBinaryTransform implementations.
    Generic tests are provided here, and sub-classes must implement the _get*_fixture() method(s)
    to provide the test data for a given test method.  For example,  get_test_transform_fixtures()
    provides the test data for the test_transform() test method.
    """

    def _install_test_fixtures(self, metafunc):
        # Apply the fixtures for the method with these input names (i.e. test_transform()).
        if (
            "transform" in metafunc.fixturenames
            and "in_binary_list" in metafunc.fixturenames
            and "expected_binary_list" in metafunc.fixturenames
            and "expected_metadata_list" in metafunc.fixturenames
        ):
            # Let the sub-class define the specific tests and test data for the transform under test.
            f = self.get_test_transform_fixtures()
            # Install the fixture, matching the parameter names used by test_transform() method.
            metafunc.parametrize("transform,in_binary_list,expected_binary_list,expected_metadata_list", f)

    def test_transform(
        self,
        transform: AbstractBinaryTransform,
        in_binary_list: list[tuple[str, bytes]],
        expected_binary_list: list[tuple[bytes, str]],
        expected_metadata_list: list[dict[str, float]],
    ):
        """
        Use the given transform to transform() the given binary file and compare the results (list of binary files and
        metadata) with the expected values as given.  The inputs are provided by the sub-class definition of
        get_test_transform_fixtures().
        :param transform: transform to test.
        :param in_binary_list:  table(s) to transform
        :param expected_binary_list: the expected accumulation of output files produced by the transform() call.
            This should include any empty files if some of the calls to transform_binary() generate empty files.
            If the final call to flush() produces an empty list of files, these will not be included here (duh!).
            However, see expected_metadata_list for the handling of metadata produced by flush().
        :param expected_metadata_list: the expected list of accumulated metadata dictionaries across all calls to
            transform() and the final call to flush().  Transforms that produce nothing from flush() should include
            and empty dictionary at the end of this list.
        :return:
        """
        all_files_list = []
        all_metadata_list = []
        for in_file in in_binary_list:
            files_list, metadata = transform.transform_binary(file_name=in_file[0], byte_array=in_file[1])
            all_files_list.extend(files_list)
            all_metadata_list.append(metadata)

        files_list, metadata = transform.flush_binary()
        all_files_list.extend(files_list)
        all_metadata_list.append(metadata)

        AbstractBinaryTransformTest.validate_expected_files(all_files_list, expected_binary_list)
        AbstractBinaryTransformTest.validate_expected_metadata_lists(all_metadata_list, expected_metadata_list)

    def get_test_transform_fixtures(self) -> list[Tuple]:
        """
        Get the test data for the test_transform() test.
        :return:  a list of Tuples, to test. Each tuple contains the test inputs for test_transform() method.
            Item 0: The AbstractBinaryTransform to be tested
            Item 1: The input file(s) to be transformed
            Item 2: The expected list of output file(s) for transformation of the input.
            Item 3: the expected metadata for transformation of the input.
        """
        raise NotImplemented()
