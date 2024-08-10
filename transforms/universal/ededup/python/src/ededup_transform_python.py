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

import pyarrow as pa

from ededup_transform_base import EdedupTransformBase, EdedupTransformConfigurationBase, HashFilter
from data_processing.runtime.pure_python.runtime_configuration import PythonTransformRuntimeConfiguration
from data_processing.runtime.pure_python import PythonTransformLauncher


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
        self.filter = HashFilter({})
        self.stats = config.get("statistics", None)

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

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        We are implementing flush here to compute additional statistics
        :return: additional metadata
        """
        h_size, h_memory = self.filter.get_hash_size()
        m_data = {"number of hashes": h_size, "hash memory, GB": h_memory}
        if self.stats is not None:
            stats = self.stats.get_execution_stats()
            dedup_prst = 100 * (1.0 - stats.get("result_documents", 1) / stats.get("source_documents", 1))
            m_data = m_data | {"de duplication %": dedup_prst}
        return [], m_data


class EdedupPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=EdedupTransformConfigurationBase(transform_class=EdedupPythonTransform))


if __name__ == "__main__":
    launcher = PythonTransformLauncher(EdedupPythonTransformConfiguration())
    launcher.launch()
