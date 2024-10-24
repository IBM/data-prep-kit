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
from data_processing_spark.runtime.spark import (
    SparkTransformLauncher,
    SparkTransformRuntimeConfiguration,
)
from signature_calc_transform import SignatureCalculationTransformConfiguration


logger = get_logger(__name__)


class SignatureCalculationSparkTransformConfiguration(SparkTransformRuntimeConfiguration):
    """
    Implements the SparkTransformConfiguration for Fuzzy Dedup Signature Calculation
    as required by the PythonTransformLauncher.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=SignatureCalculationTransformConfiguration())


if __name__ == "__main__":
    # create launcher
    launcher = SparkTransformLauncher(runtime_config=SignatureCalculationSparkTransformConfiguration())
    logger.info("Launching fuzzy dedup signature calculation transform")
    # Launch the spark worker(s) to process the input
    launcher.launch()
