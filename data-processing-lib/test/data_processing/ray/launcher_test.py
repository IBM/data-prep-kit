import os
import sys

from data_processing.ray import *
from data_processing.ray.launcher import TransformLauncher
from data_processing.ray.transform_runtime import AbstractTableTransformRuntimeFactory, DefaultTableTransformRuntime
from data_processing.table_transform import AbstractTableTransform
from data_processing.utils import *


"""
 see: https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments
 to run test using argparse we can simply overwrite sys.argv to supply required arguments
"""
s3_cred = {
    "access_key": "key",
    "secret_key": "secret",
    "cos_url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
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

    def submit_for_execution(self) -> int:
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
        "max_files": -1,
        "worker_options": TestUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "checkpointing": False,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": TestUtils.convert_to_ast(code_location),
    }
    # cos not defined
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    # Add S3 configuration
    params["s3_config"] = TestUtils.convert_to_ast(s3_conf)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    # Add S3 credentials
    params["s3_cred"] = TestUtils.convert_to_ast(s3_cred)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 0 == res
    # Add lake house
    params["lh_config"] = TestUtils.convert_to_ast(lakehouse_conf)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    # Add local config, should fail because now three different configs exist
    params["local_config"] = TestUtils.convert_to_ast(local_conf)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    # remove local config, should still fail, because two configs left
    del params["local_config"]
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res

    # remove s3 config, now it should work
    del params["s3_config"]
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 0 == res


def test_local_config():
    # test that the driver works with local configuration
    params = {
        "run_locally": True,
        "max_files": -1,
        "worker_options": TestUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "checkpointing": False,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": TestUtils.convert_to_ast(code_location),
    }
    params["local_config"] = TestUtils.convert_to_ast(local_conf)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 0 == res


def test_local_config_validate():
    # test the validation of the local configuration
    params = {
        "run_locally": True,
        "max_files": -1,
        "worker_options": TestUtils.convert_to_ast(worker_options),
        "num_workers": 5,
        "checkpointing": False,
        "pipeline_id": "pipeline_id",
        "job_id": "job_id",
        "creation_delay": 0,
        "code_location": TestUtils.convert_to_ast(code_location),
    }
    # invalid local configurations, driver launch should fail with any of these
    local_conf_empty = {}
    local_conf_no_input = {
        "output_folder": os.path.join(os.sep, "tmp", "output"),
    }
    local_conf_no_output = {
        "input_folder": os.path.join(os.sep, "tmp", "input"),
    }
    params["local_config"] = TestUtils.convert_to_ast(local_conf_empty)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    params["local_config"] = TestUtils.convert_to_ast(local_conf_no_input)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    params["local_config"] = TestUtils.convert_to_ast(local_conf_no_output)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 1 == res
    params["local_config"] = TestUtils.convert_to_ast(local_conf)
    sys.argv = TestUtils.dict_to_req(d=params)
    res = TestLauncher(
        name="test",
        transformer_factory=AbstractTableTransformRuntimeFactory(
            runtime_class=DefaultTableTransformRuntime, transformer_class=AbstractTableTransform
        ),
    ).execute()
    assert 0 == res
