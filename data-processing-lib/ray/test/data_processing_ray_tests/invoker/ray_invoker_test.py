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

from data_processing.utils import ParamsUtils, TransformsConfiguration, get_logger
from data_processing_ray.runtime.ray import execute_ray_transform


logger = get_logger(__name__)


def test_configuration():
    """
    test configuration population
    :return:
    """
    t_configuration = TransformsConfiguration()
    transforms = t_configuration.get_available_transforms()
    logger.info(f"available transforms {transforms}")
    assert len(transforms) == 13


def test_execution():
    input_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../../../transforms/universal/noop/ray/test-data/input")
    )
    output_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../../../transforms/universal/noop/ray/output")
    )
    worker_options = {"num_cpus": 0.8}
    t_configuration = TransformsConfiguration()
    res = execute_ray_transform(
        configuration=t_configuration,
        name="noop",
        input_folder=input_dir,
        output_folder=output_dir,
        params={
            "noop_sleep_sec": 1,
            "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
            "runtime_num_workers": 3,
        },
    )
    assert res == True
