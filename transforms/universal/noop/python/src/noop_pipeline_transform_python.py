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

from data_processing.runtime.pure_python import PythonTransformLauncher, PythonTransformRuntimeConfiguration
from data_processing.transform.pure_python import PythonPipelineTransform
from data_processing.transform import PipelineTransformConfiguration
from data_processing.utils import get_logger
from noop_transform_python import NOOPPythonTransformConfiguration

logger = get_logger(__name__)


class NOOPPypelinePythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=PipelineTransformConfiguration(
            config={"transforms": [NOOPPythonTransformConfiguration()]},
            transform_class=PythonPipelineTransform))


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = PythonTransformLauncher(NOOPPypelinePythonTransformConfiguration())
    logger.info("Launching resize/noop transform")
    launcher.launch()
