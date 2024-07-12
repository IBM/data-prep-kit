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

from typing import Any

from data_processing.runtime import AbstractTransformFileProcessor
from data_processing.data_access import data_access
from data_processing.transform import AbstractBinaryTransform


class SparkTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the actual work/actor processing of a single file
    """

    def __init__(self, d_access: data_access, transform_params: dict[str, Any],
                 transform_class: type[AbstractBinaryTransform], statistics: dict[str, Any]):
        """
        Init method
        """
        super().__init__()
        # Create data access
        self.data_access = d_access
        # Add data access ant statistics to the processor parameters
        transform_params["data_access"] = self.data_access
        # Create local processor
        self.transform = transform_class(transform_params)
        # Create statistics
        self.stats = statistics

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        for key, val in stats.items():
            if val > 0:
                self.stats[key] = self.stats.get(key, 0) + val
