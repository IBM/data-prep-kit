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
from argparse import Namespace, ArgumentParser
import ray
from ray.actor import ActorHandle
from argparse import Namespace
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
    RayUtils,
)
from fdedup.utils import DocCollector
from fdedup.transforms.base import (FdedupFilterTransformBase,
                                    FdedupFilterTransformConfigurationBase,
                                    doc_id_snapshot_directory_key, filter_cli_prefix,
                                    doc_id_cache_key,
                                    )
from fdedup_ray.transforms import docid_cpu_key, num_docid_key


filter_docid_cpu_cli_param = f"{filter_cli_prefix}{docid_cpu_key}"
filter_num_docid_cli_param = f"{filter_cli_prefix}{num_docid_key}"


class FdedupFilterTransformRay(FdedupFilterTransformBase):
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
        request = [[] for _ in range(len(self.doc_id_cache))]
        for value in ids:
            doc_id = value
            request[doc_id % len(self.doc_id_cache)].append(doc_id)
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.doc_id_cache[i].filter.remote(req))
            i += 1
        # Process replies
        unique = {}
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            reply = ray.get(ready)[0]
            unique.update(reply)
            remote_replies = not_ready
        return unique


class FdedupFilterRuntimeRay(DefaultRayTransformRuntime):
    """
    fuzzy dedup filter runtime support
    """

    def __init__(self, params: dict[str, Any]):
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        super().__init__(params=params)
        self.doc_collector = None
        self.n_docid = params.get(num_docid_key, 1)
        self.docid_cpu = params.get(docid_cpu_key, .5)


    def get_transform_config(
            self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
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
            doc_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}docs"
        else:
            doc_path = snapshot_path
        self.doc_collector = [None] * self.n_docid
        files, retries = data_access.get_folder_files(path=doc_path)
        if retries > 0:
            statistics.add_stats.remote({"data access retries": retries})
        for file in files.keys():
            i = int(file[file.rfind("_") + 1:])
            self.doc_collector[i] = ray.remote(DocCollector).options(**{"num_cpus": self.docid_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": file}
            )
        self.logger.info(f"Created {len(self.doc_collector)} doc collectors")
        return self.params | {doc_id_cache_key: self.doc_collector}

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Update/augment the given stats object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute and add additional statistics
        dedup_prst = 100 * (1.0 - stats.get("result_documents", 1) / stats.get("source_documents", 1))
        return {"de duplication %": dedup_prst} | stats


class FdedupFilterTransformConfigurationRay(FdedupFilterTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(transform_class=FdedupFilterTransformRay)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        super().add_input_params(parser)
        parser.add_argument(
            f"--{filter_docid_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per doc-id hash"
        )
        parser.add_argument(
            f"--{filter_num_docid_cli_param}",
            type=int,
            default=1,
            help="number of doc id caches to use"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        super().apply_input_params(args=args)
        self.logger.info(f"fuzzy dedup filter params are {self.params}")
        return True


class FdedupFilterRayTransformRuntimeConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=FdedupFilterTransformConfigurationRay(),
            runtime_class=FdedupFilterRuntimeRay,
        )


if __name__ == "__main__":
    launcher = RayTransformLauncher(FdedupFilterRayTransformRuntimeConfiguration())
    launcher.launch()
