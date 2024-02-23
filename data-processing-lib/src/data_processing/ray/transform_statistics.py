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

    def add_stats(self, stats=dict[str, Any]) -> None:
        """
        Add statistics
        :param stats - dictionary creating new statistics
        :return: None
        """
        for key, val in stats.items():
            self.stats[key] = self.stats.get(key, 0) + val
            if key == "source_files":
                self.source_document_counter.inc(val)
            if key == "source_size":
                self.data_read_counter.inc(val)
            if key == "result_files":
                self.result_document_counter.inc(val)
            if key == "result_size":
                self.data_write_counter.inc(val)

    def get_execution_stats(self) -> dict[str, Any]:
        """
        Get execution statistics
        :return:
        """
        return self.stats
