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

from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics
from data_processing.utils import (
    UnrecoverableException,
    SnapshotUtils,
    get_logger
)

from data_processing.runtime.pure_python import (
    PythonTransformLauncher,
    PythonTransformRuntimeConfiguration,
    DefaultPythonTransformRuntime,
)

from ededup_transform_base import (
    EdedupTransformBase,
    EdedupTransformConfigurationBase,
    HashFilter,
)


logger = get_logger(__name__)

class EdedupPythonTransform(EdedupTransformBase):
    """
    Implements dedup table transformer.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of the doc column
        """
        super().__init__(config)
        self.filter = config.get("filter", None)
        if self.filter is None:
            raise UnrecoverableException("filter is not provided")

    def _process_cached_hashes(self, hd: dict[str, str]) -> list[str]:
        """
        check hashes uniqueness with the distributed cache of hashes
        :param hd: dictionary of hash to document
        :return: unique documents
        """
        unique_hashes = self.filter.get_unique(ha=list(hd.keys()))
        unique = []
        for uh in unique_hashes:
            unique.append(hd[uh])
        return unique


class EdedupPythonRuntime(DefaultPythonTransformRuntime):
    """
    Exact dedup runtime support
    """
    def __init__(self, params: dict[str, Any]):
        super().__init__(params=params)
        self.filter = None

    def get_transform_config(
            self, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics, files: list[str]
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc., that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        if self.params.get("use_snapshot", False):
            snapshot_path = self.params.get("snapshot_directory", None)
            if snapshot_path is None or len(snapshot_path) == 0:
                snapshot_path = f"{SnapshotUtils.get_snapshot_folder(data_access_factory.create_data_access())}hash_collector_1"
            else:
                snapshot_path = f"{snapshot_path}/hash_collector_1"
            logger.info(f"continuing from the hash snapshot {snapshot_path}")
            self.filter = HashFilter({"data_access_factory": data_access_factory, "id": 1, "snapshot": snapshot_path})
        else:
            logger.info("Starting from the beginning")
            self.filter = HashFilter({"data_access_factory": data_access_factory, "id": 1})
        return self.params | {"filter": self.filter}

    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        h_size, h_memory = self.filter.get_hash_size()
        stats.add_stats({"number of hashes": h_size, "hash memory, GB": h_memory})
        current = stats.get_execution_stats()
        dedup_prst = 100 * (1.0 - current.get("result_documents", 1) / current.get("source_documents", 1))
        stats.add_stats({"de duplication %": dedup_prst})
        self.filter.snapshot()



class EdedupPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=EdedupTransformConfigurationBase(transform_class=EdedupPythonTransform),
            runtime_class=EdedupPythonRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(EdedupPythonTransformConfiguration())
    launcher.launch()
