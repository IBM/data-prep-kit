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

from data_processing.utils import get_logger
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from license_copyright_removal_transform import LicenseCopyrightRemovalTransformConfiguration


logger = get_logger(__name__)


class LicenseCopyrightRemovalRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=LicenseCopyrightRemovalTransformConfiguration())


if __name__ == "__main__":
    launcher = RayTransformLauncher(LicenseCopyrightRemovalRayTransformConfiguration())
    logger.info("Launching license and copyright removal")
    launcher.launch()