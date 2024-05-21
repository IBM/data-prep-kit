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

import ray
from data_processing.runtime import AbstractTransformFileProcessor


@ray.remote(scheduling_strategy="SPREAD")
class RayTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the actual work/actor processing of a single file
    """

    def __init__(self, params: dict[str, Any]):
        """
        Init method
        :param params: dictionary that has the following key
            data_access_factory: data access factory
            transform_class: local transform class
            transform_params: dictionary of parameters for local transform creation
            statistics: object reference to statistics
        """
        super().__init__()
        # Create data access
        self.data_access = params.get("data_access_factory", None).create_data_access()
        # Add data access ant statistics to the processor parameters
        transform_params = params.get("transform_params", None)
        transform_params["data_access"] = self.data_access
        # Create local processor
        self.transform = params.get("transform_class", None)(transform_params)
        # Create statistics
        self.stats = params.get("statistics", None)

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        self.stats.add_stats.remote(stats)
