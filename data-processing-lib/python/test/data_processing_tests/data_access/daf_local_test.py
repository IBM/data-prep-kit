import os

from data_processing.test_support.data_access import AbstractDataAccessFactoryTests
from data_processing.utils import ParamsUtils


class TestDataAccessFactory(AbstractDataAccessFactoryTests):
    def _get_io_params(self):  # -> str, dict:
        """
        Get the dictionary of parameters to configure the DataAccessFactory to produce the DataAccess implementation
        to be tested and the input/output folders. The tests expect the input and output to be as structured in
        test-data/data_processing/daf directory tree.
        Returns:
            str : input folder path
            dict: cli parameters to configure the DataAccessFactory with
        """
        params = {}
        input_folder = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../test-data", "data_processing", "daf", "input")
        )
        output_folder = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../test-data", "data_processing", "daf", "output")
        )
        local_conf = {
            "input_folder": input_folder,
            "output_folder": output_folder,
        }
        params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf)
        return input_folder, params
