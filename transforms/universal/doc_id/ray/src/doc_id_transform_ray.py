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
from data_processing.data_access import DataAccessFactoryBase
from data_processing.utils import UnrecoverableException
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle
from doc_id_transform_base import (IDGenerator,
                                   DocIDTransformBase,
                                   DocIDTransformConfigurationBase,
                                   start_id_key,
                                   id_generator_key,
                                   )


class DocIDRayTransform(DocIDTransformBase):
    """
    Implements schema modification of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        super().__init__(config)
        self.id_generator = config.get(id_generator_key, None)
        if self.id_generator is None and self.int_column is not None:
            raise UnrecoverableException(
                "There is no id generating actor defined."
            )

    def _get_starting_id(self, n_rows: int) -> int:
        """
        Get starting ID
        :param n_rows - number of rows in the table
        :return: starting id for the table
        """
        return ray.get(self.id_generator.get_ids.remote(n_rows))


class DocIDRayRuntime(DefaultRayTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            doc_column - name of the doc column
            hash_column - name of doc id column to create
            int_column - name of integer doc id column to create
        """
        super().__init__(params)
        self.id_generator = None

    def get_transform_config(
        self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - statistics actor reference
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        # create id generator
        self.id_generator = ray.remote(IDGenerator).options(num_cpus=0.25).remote(self.params.get(start_id_key, 1))
        return self.params | {id_generator_key: self.id_generator}

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Update/augment the given stats object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute and add additional statistics
        stats["final id"] = ray.get(self.id_generator.get_current.remote())
        return stats


class DocIDRayTransformConfiguration(DocIDTransformConfigurationBase):
    def __init__(self):
        super().__init__(transform_class=DocIDRayTransform)


class DocIDRayTransformRuntimeConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=DocIDRayTransformConfiguration(),
            runtime_class=DocIDRayRuntime
        )


if __name__ == "__main__":
    launcher = RayTransformLauncher(DocIDRayTransformRuntimeConfiguration())
    launcher.launch()
