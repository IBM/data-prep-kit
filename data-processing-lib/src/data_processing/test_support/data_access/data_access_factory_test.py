import os
import sys
from argparse import ArgumentParser

from data_processing.data_access import DataAccessFactory
from data_processing.utils import ParamsUtils


def _verify_files(rootdir, expected_files, found_files):
    for file in expected_files:
        full_path_expected = os.path.abspath(os.path.join(rootdir, file))
        assert full_path_expected in found_files
    assert len(expected_files) == len(found_files)


class AbstractDataAccessFactoryTests:
    def test_get_all(self):
        params = {}
        expected_files = ["ds1/sample1.parquet", "ds1/sample2.parquet", "ds2/sample3.parquet"]
        self._run_test(params, expected_files)

    def test_nsamples(self):
        params = {}
        expected_files = 2
        params["data_num_samples"] = expected_files
        self._run_test(params, expected_files)

    def test_data_sets(self):
        params = {}
        params["data_data_sets"] = ["ds1"]
        expected_files = ["ds1/sample1.parquet", "ds1/sample2.parquet"]
        self._run_test(params, expected_files)

    def test_files_to_use(self):
        params = {}
        params["data_files_to_use"] = [".nothere"]
        expected_files = []
        self._run_test(params, expected_files)

    def test_checkpoint(self):
        params = {}
        params["data_checkpointing"] = "True"
        expected_files = ["ds1/sample2.parquet", "ds2/sample3.parquet"]
        self._run_test(params, expected_files)

    def _get_io_params(self):  # -> str, dict:
        """
        Get the dictionary of parameters to configure the DataAccessFactory to produce the DataAccess implementation
        to be tested and the input/output folders. The tests expect the input and output to be as structured in
        test-data/data_processing/daf directory tree.
        Returns:
            str : input folder path
            dict: cli parameters to configure the DataAccessFactory with
        """
        raise ValueError("Must be implemented")

    def _run_test(self, params, expected_files):
        input_folder, io_params = self._get_io_params()
        params = params | io_params

        # Set the simulated command line args
        sys.argv = ParamsUtils.dict_to_req(params)
        daf = DataAccessFactory()
        parser = ArgumentParser()
        daf.add_input_params(parser)
        args = parser.parse_args()
        daf.apply_input_params(args)
        data_access = daf.create_data_access()
        files, metadata = data_access.get_files_to_process()
        if isinstance(expected_files, int):
            assert len(files) == expected_files
        else:
            _verify_files(input_folder, expected_files, files)
