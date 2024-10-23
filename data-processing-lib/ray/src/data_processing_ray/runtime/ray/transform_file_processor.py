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
from data_processing.utils import UnrecoverableException


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
        super().__init__(
            data_access_factory=params.get("data_access_factory", None),
            transform_parameters=dict(params.get("transform_params", {})),
            is_folder=params.get("is_folder", False)
        )
        # Create statistics
        self.stats = params.get("statistics", None)
        if self.stats is None:
            self.logger.error("Transform file processor: statistics is not specified")
            raise UnrecoverableException("statistics is None")
        self.transform_params["statistics"] = self.stats
        # Create local processor
        try:
            self.transform = params.get("transform_class", None)(self.transform_params)
        except Exception as e:
            self.logger.error(f"Exception creating transform  {e}")
            raise UnrecoverableException("failed creating transform")

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        self.stats.add_stats.remote(stats)
