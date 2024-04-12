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
from ray.util.metrics import Counter


@ray.remote(num_cpus=0.25, scheduling_strategy="SPREAD")
class TransformStatistics(object):
    """
    Basic statistics class collecting basic execution statistics.
    It can be extended for specific processors
    """

    def __init__(self, params: dict[str, Any]):
        """
        Init - setting up variables All of the statistics is collected in the dictionary
        """
        self.stats = {}
        self.data_write_counter = Counter("data_written", "Total data written bytes")
        self.data_read_counter = Counter("data_read", "Total data read bytes")
        self.source_document_counter = Counter("source_files_processed", "Total source document processed")
        self.result_document_counter = Counter("result_files_written", "Total result documents written")
        self.empty_table_counter = Counter("empty_tables", "Total empty tables read")
        self.failed_read_counter = Counter("failed_read_files", "Total read failed files")
        self.failed_write_counter = Counter("failed_write_files", "Total write failed files")
        self.transform_exceptions_counter = Counter("transform_exceptions", "Transform exception occurred")

    def add_stats(self, stats=dict[str, Any]) -> None:
        """
        Add statistics
        :param stats - dictionary creating new statistics
        :return: None
        """
        for key, val in stats.items():
            if val > 0:
                self.stats[key] = self.stats.get(key, 0) + val
                if key == "source_files":
                    self.source_document_counter.inc(val)
                if key == "source_size":
                    self.data_read_counter.inc(val)
                if key == "result_files":
                    self.result_document_counter.inc(val)
                if key == "skipped empty tables":
                    self.empty_table_counter.inc(val)
                if key == "failed_reads":
                    self.failed_read_counter.inc(val)
                if key == "failed_writes":
                    self.failed_write_counter.inc(val)
                if key == "transform execution exception":
                    self.transform_exceptions_counter.inc(val)

    def get_execution_stats(self) -> dict[str, Any]:
        """
        Get execution statistics
        :return:
        """
        return self.stats
