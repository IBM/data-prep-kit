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

# Collection of code data specific annotations and its heuristics are borrowed from:
# CodeParrot  https://github.com/huggingface/transformers/tree/main/examples/research_projects/codeparrot#preprocessing
# BigCode Dataset https://github.com/bigcode-project/bigcode-dataset/tree/main/preprocessing
#
# Code specific heuristics like alpha numeric, char token ratio implementations & others are taken from CodeParrot and BigCode Dataset
# preprocessing scripts and modified according to data-prep-kit specific framework.


import os

from code_quality_transform import CodeQualityTransformConfiguration
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)


class CodeQualityRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=CodeQualityTransformConfiguration())


if __name__ == "__main__":
    launcher = RayTransformLauncher(CodeQualityRayTransformConfiguration())
    launcher.launch()
