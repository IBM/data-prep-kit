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
from data_processing.transform import TransformStatistics


@ray.remote(num_cpus=0.25, scheduling_strategy="SPREAD")
class TransformStatisticsRay(TransformStatistics):
    """
    Basic statistics class collecting basic execution statistics.
    It can be extended for specific processors
    """

    def __init__(self, params: dict[str, Any]):
        from ray.util.metrics import Counter

        super().__init__()
        self.data_write_counter = Counter(name="data_written", description="Total data written bytes")
        self.data_read_counter = Counter(name="data_read", description="Total data read bytes")
        self.source_files_counter = Counter(name="source_files_processed", description="Total source files processed")
        self.result_files_counter = Counter(name="result_files_written", description="Total result files written")
        self.source_documents_counter = Counter(
            name="source_documents_processed", description="Total source document processed"
        )
        self.result_documents_counter = Counter(
            name="result_documents_written", description="Total result documents written"
        )
        self.empty_table_counter = Counter(name="empty_tables", description="Total empty tables read")
        self.failed_read_counter = Counter(name="failed_read_files", description="Total read failed files")
        self.failed_write_counter = Counter(name="failed_write_files", description="Total write failed files")
        self.transform_exceptions_counter = Counter(
            name="transform_exceptions", description="Transform exception occurred"
        )
        self.data_retries_counter = Counter(name="data_access_retries", description="Data access retries")

    def add_stats(self, stats=dict[str, Any]) -> None:
        """
        Add statistics
        :param stats - dictionary creating new statistics
        :return: None
        """
        for key, val in stats.items():
            self.stats[key] = self.stats.get(key, 0) + val
            if val > 0:
                if key == "source_files":
                    self.source_files_counter.inc(val)
                if key == "source_size":
                    self.data_read_counter.inc(val)
                if key == "result_files":
                    self.result_files_counter.inc(val)
                if key == "source_doc_count":
                    self.source_documents_counter.inc(val)
                if key == "result_doc_count":
                    self.result_documents_counter.inc(val)
                if key == "skipped empty tables":
                    self.empty_table_counter.inc(val)
                if key == "failed_reads":
                    self.failed_read_counter.inc(val)
                if key == "failed_writes":
                    self.failed_write_counter.inc(val)
                if key == "transform execution exception":
                    self.transform_exceptions_counter.inc(val)
                if key == "data access retries":
                    self.data_retries_counter.inc(val)
