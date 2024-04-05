import os
import sys

from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import ParamsUtils


"""
 see: https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments
 to run test using argparse we can simply overwrite sys.argv to supply required arguments
"""
s3_cred = {
    "access_key": "key",
    "secret_key": "secret",
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "cos-optimal-llm-pile/sanity-test/input/dataset=text/",
    "output_folder": "cos-optimal-llm-pile/boris-test/",
}
lakehouse_conf = {
    "input_table": "foo",
    "input_dataset": "dataset",
    "input_version": "version",
    "output_table": "blah",
    "output_path": "path",
    "token": "token",
    "lh_environment": "PROD",
}
local_conf = {
    "input_folder": os.path.join(os.sep, "tmp", "input"),
    "output_folder": os.path.join(os.sep, "tmp", "output"),
}

worker_options = {"num_cpu": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}


class TestLauncher(TransformLauncher):
    """
    Test driver for validation of the functionality
    """

    def _submit_for_execution(self) -> int:
        """
        Overwrite this method to just print all parameters to make sure that everything works
        :return:
        """
        print("\n\nPrinting preprocessing parameters")
        print(f"Run locally {self.run_locally}")
        return 0


def test_launcher():
    params = {
        "run_locally": True,
        "worker_options": ParamsUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": ParamsUtils.convert_to_ast(code_location),
        "data_max_files": -1,
        "data_checkpointing": False,
    }
    # cos not defined
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 0 == res
    # Add S3 configuration
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    # Add S3 credentials
    params["data_s3_cred"] = ParamsUtils.convert_to_ast(s3_cred)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 0 == res
    # Add lake house
    params["data_lh_config"] = ParamsUtils.convert_to_ast(lakehouse_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    # Add local config, should fail because now three different configs exist
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    # remove local config, should still fail, because two configs left
    del params["data_local_config"]
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res

    # remove s3 config, now it should work
    del params["data_s3_config"]
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 0 == res
    sys.argv = []


def test_local_config():
    # test that the driver works with local configuration
    params = {
        "run_locally": True,
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        "data_max_files": -1,
        "data_checkpointing": False,
        "worker_options": ParamsUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": ParamsUtils.convert_to_ast(code_location),
    }
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 0 == res


def test_local_config_validate():
    # test the validation of the local configuration
    params = {
        "run_locally": True,
        "data_max_files": -1,
        "data_checkpointing": False,
        "worker_options": ParamsUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": ParamsUtils.convert_to_ast(code_location),
    }
    # invalid local configurations, driver launch should fail with any of these
    local_conf_empty = {}
    local_conf_no_input = {"output_folder": "output_folder"}
    local_conf_no_output = {"input_folder": "input_folder"}
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf_empty)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    print(f"parameters {sys.argv}")
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf_no_input)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf_no_output)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res  
    params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 0 == res


def test_s3_config_validate():
    # test the validation of the local configuration
    params = {
        "run_locally": True,
        "data_max_files": -1,
        "data_checkpointing": False,
        "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
        "worker_options": ParamsUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": ParamsUtils.convert_to_ast(code_location),
    }
    # invalid local configurations, driver launch should fail with any of these
    s3_conf_empty = {}
    s3_conf_no_input = {"output_folder": "output_folder"}
    s3_conf_no_output = {"input_folder": "input_folder"}
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf_empty)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    print(f"parameters {sys.argv}")
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf_no_input)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf_no_output)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 1 == res
    params["data_s3_config"] = ParamsUtils.convert_to_ast(s3_conf)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    res = TestLauncher(
        transform_runtime_config=DefaultTableTransformConfiguration(
            name="test", runtime_class=DefaultTableTransformRuntime, transform_class=AbstractTableTransform
        ),
    ).launch()
    assert 0 == res
