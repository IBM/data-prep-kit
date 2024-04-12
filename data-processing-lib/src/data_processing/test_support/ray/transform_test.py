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

import sys
import tempfile
from typing import Any

from data_processing.ray import DefaultTableTransformConfiguration, TransformLauncher
from data_processing.test_support.abstract_test import AbstractTest
from data_processing.utils import ParamsUtils


class AbstractTransformLauncherTest(AbstractTest):
    """
    The Ray-based test class for all/most AbstractTransform implementations.
    Generic tests are provided here, and sub-classes must implement the _get*_fixture() method(s)
    to provide the test data for a given test method.  For example,  get_test_transform_fixtures()
    provides the test data for the test_transform() test method.

    """

    @staticmethod
    def _get_argv(cli_params: dict[str, Any], in_table_path: str, out_table_path: str):
        args = {} | cli_params
        local_ast = {"input_folder": in_table_path, "output_folder": out_table_path}
        args["data_local_config"] = local_ast
        args["run_locally"] = "True"
        argv = ParamsUtils.dict_to_req(args)
        return argv

    def test_transform(
        self,
        transform_config: DefaultTableTransformConfiguration,
        cli_params: dict[str, Any],
        in_table_path: str,
        expected_out_table_path: str,
    ):
        """
        Test the given transform and its runtime using the given CLI arguments, input directory of data files and expected output directory.
        Data is processed into a temporary output directory which is then compared with the directory of expected output.
        :param transform_config:
        :param cli_params: a map of the simulated CLI arguments (w/o --).  This includes both the transform-specific CLI parameters and  the Ray launching args.
        :param in_table_path: a directory containing the input parquet files to be processed and results compared against the expected output table path.
        :param expected_out_table_path: directory contain parquet and metadata.json that is expected to match the processed input directory.
        :return:
        """

        launcher = TransformLauncher(transform_config)
        prefix = transform_config.get_name()
        with tempfile.TemporaryDirectory(prefix=prefix, dir="/tmp") as temp_dir:
            print(f"Using temporary output path {temp_dir}")
            sys.argv = self._get_argv(cli_params, in_table_path, temp_dir)
            launcher.launch()
            AbstractTest.validate_directory_contents(temp_dir, expected_out_table_path)

    def _install_test_fixtures(self, metafunc):
        # Apply the fixtures for the method with these input names (i.e. test_transform()).
        if (
            "transform_config" in metafunc.fixturenames
            and "cli_params" in metafunc.fixturenames
            and "in_table_path" in metafunc.fixturenames
            and "expected_out_table_path" in metafunc.fixturenames
        ):
            # Let the sub-class define the specific tests and test data for the transform under test.
            f = self.get_test_transform_fixtures()
            # Install the fixture, matching the parameter names used by test_transform() method.
            metafunc.parametrize("transform_config,cli_params,in_table_path,expected_out_table_path", f)

    def get_test_transform_fixtures(self) -> list[tuple]:
        """
        Get the test data for the test_transform() test.  The returned list contains 0 or more tuples
        containing the following:
            |  Item 0: The DefaultTableTransformConfiguration to be tested. This defines the Transform being tested and the Runtime required to run it.
            |  Item 1: The dictionary of command line args to simulate when running the transform.
            |  Item 2: The input path to the parquet files to process.
            |  Item 3: the output path holding the expected results of the transform including parquet and metadata.json
        :return:  a list of Tuples, to test. Each tuple contains the test inputs for test_transform() method.
        """
        raise NotImplemented()
