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

from data_processing.runtime import AbstractTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher
from filter_transform import (
    filter_columns_to_drop_cli_param,
    filter_criteria_cli_param,
    filter_logical_operator_cli_param,
    filter_logical_operator_default,
)
from filter_transform_ray import FilterRayTransformConfiguration

from transforms.universal.filter.python.test.test_filter_python import (
    TestPythonFilterTransform,
)


class TestRayFilterTransform(TestPythonFilterTransform):
    """
    Extends the Python super-class to redefine the launcher as a RayTransformLauncher.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def _get_launcher(self) -> AbstractTransformLauncher:
        return RayTransformLauncher(FilterRayTransformConfiguration())
