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
    "access_key": "access",
    "secret_key": "secret",
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "input_folder",
    "output_folder": "output_folder",
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
        "data_max_files": -1,
        "data_checkpointing": False,
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 5,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    }
    # s3 not defined
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
    assert 0 == res


def test_local_config():
    # test that the driver works with local configuration
    params = {
        "run_locally": True,
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        "data_max_files": -1,
        "data_checkpointing": False,
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 5,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
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
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 5,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
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
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 5,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
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
