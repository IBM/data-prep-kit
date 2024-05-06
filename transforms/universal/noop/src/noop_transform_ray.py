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

from argparse import ArgumentParser, Namespace

from data_processing.ray import TableTransformConfigurationRay, TransformLauncherRay
from data_processing.ray.transform_runtime import TableTransformConfigurationRay2
from data_processing.utils import get_logger
from noop_transform import NOOPTransformConfiguration


logger = get_logger(__name__)


class NOOPTransformConfigurationRay(TableTransformConfigurationRay2):
    def __init__(self):
        params = {}
        super().__init__(NOOPTransformConfiguration())


if __name__ == "__main__":
    launcher = TransformLauncherRay(transform_runtime_config=NOOPTransformConfigurationRay())
    logger.info("Launching noop transform")
    launcher.launch()
