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
from argparse import Namespace
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing.transform import TransformStatistics
from data_processing.runtime.pure_python import (DefaultPythonTransformRuntime,
                                                 PythonTransformLauncher,
                                                 PythonTransformRuntimeConfiguration
                                                 )
from fdedup.utils import DocCollector
from fdedup.transforms.base import (FdedupFilterTransformBase,
                                    FdedupFilterTransformConfigurationBase,
                                    doc_id_snapshot_directory_key,
                                    doc_id_cache_key,
                                    )


class FdedupFilterTransform(FdedupFilterTransformBase):
    """
    Fdedup filter Python version
    """
    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters, with the following keys
        doc_column - name of doc column
        doc_id_int_column - name of int doc id column
        doc_id_cache - doc id cache
        """
        # superclass initialization
        super().__init__(config)

    def _get_unique_ids(self, ids: list[int]) -> dict[int, int]:
        """
        Get unique IDs
        :param ids: table ids
        :return: unique ids and clusters
        """
        return self.doc_id_cache.filter(ids.to_pylist())


class FdedupFilterRuntime(DefaultPythonTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        super().__init__(params=params)
        self.doc_collector = None

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
        data_access = data_access_factory.create_data_access()
        snapshot_path = self.params.get(doc_id_snapshot_directory_key, None)
        if snapshot_path is None or len(snapshot_path) == 0:
            doc_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}docs/doc_collector_0"
        else:
            doc_path = f"{snapshot_path}/doc_collector_0"
        self.doc_collector = DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": doc_path})
        return self.params | {doc_id_cache_key: self.doc_collector}

    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute and add additional statistics
        current = stats.get_execution_stats()
        dedup_prst = 100 * (1.0 - current.get("result_documents", 1) / current.get("source_documents", 1))
        stats.add_stats({"de duplication %": dedup_prst})


class FdedupFilterTransformConfiguration(FdedupFilterTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(transform_class=FdedupFilterTransform)

    def apply_input_params(self, args: Namespace) -> bool:
        if args.runtime_num_processors > 0:
            self.logger.info(
                f"fdedup does not support multiprocessing. Runtime_num_processors should be 0, "
                f"current {args.runtime_num_processors}"
            )
            return False
        super().apply_input_params(args=args)
        self.logger.info(f"fuzzy dedup filter params are {self.params}")
        return True


class FdedupFilterPythonTransformRuntimeConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=FdedupFilterTransformConfiguration(),
            runtime_class=FdedupFilterRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(FdedupFilterPythonTransformRuntimeConfiguration())
    launcher.launch()
