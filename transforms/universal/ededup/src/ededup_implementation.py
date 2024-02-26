from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
import ray
from data_processing.data_access import DataAccess
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    RayUtils,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import GB, TransformUtils


REQUEST_LEN = 8192


@ray.remote(scheduling_strategy="SPREAD")
class HashFilter:
    """
    Implements an element of distributed cache of hashes
    """

    def __init__(self, params: dict[str, Any]):
        """
        initialize set of local hashes
        """
        self.hashes = set()

    def get_unique(self, ha: list[str]) -> list[str]:
        """
        Get list of unique hashes
        :param ha: new set of hashes
        :return: list of unique ones
        """
        unique = []
        for h in ha:
            if h not in self.hashes:
                # If a hash does not exist, add it to unique and add to the local set
                self.hashes.add(h)
                unique.append(h)
        return unique

    def get_hash_size(self) -> tuple[int, float]:
        """
        Get size of created hashes for statistics
        :return: size of the local set and its memory footprint
        """
        return len(self.hashes), TransformUtils.deep_get_size(self.hashes) / GB


class EdedupTransform(AbstractTableTransform):
    """
    Implements dedup table transformer.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of the doc column
            hashes - list of hash actors, references
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of EdedupTableTransformConfiguration class
        super().__init__(config)
        self.doc_column = config.get("doc_column", "")
        self.hashes = config.get("hashes", [])

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        De duping table content.
        :param table: table
        :return: resulting table, statistics
        """
        # make sure that the doc column exists
        if not TransformUtils.validata_columns(table=table, required=[self.doc_column]):
            return [], {}
        # report number of source documents
        stats = {"source_documents": table.num_rows}
        # Inner variables
        hashes = set()
        unique = []
        hd = {}
        # Compute unique hashes for the table
        for text in table[self.doc_column]:
            # Compute doc hash
            h = TransformUtils.str_to_hash(TransformUtils.normalize_string(str(text)))
            if h not in hashes:  # Processing this hash for the first time
                hashes.add(h)  # Remember it locally
                hd[h] = str(text)
                if len(hd) >= REQUEST_LEN:  # time to check remotely
                    unique = unique + self._process_remote_hashes(hd=hd)
                    hd = {}
        if len(hd) > 0:  # Process remaining hashes
            unique = unique + self._process_remote_hashes(hd=hd)

        # Remove duplicates
        unique_set = set(unique)
        mask = [False] * table.num_rows
        index = 0
        for text in table[self.doc_column]:
            str_text = str(text)
            if str_text in unique_set:
                mask[index] = True
                unique_set.remove(str_text)
            index += 1
        # Create output table
        out_table = table.filter(mask)
        # report number of source documents
        stats["result_documents"] = out_table.num_rows
        return [out_table], stats

    def _process_remote_hashes(self, hd: dict[str, str]) -> list[str]:
        """
        check hashes uniqueness with the distributed cache of hashes
        :param hd: dictionary of hash to document
        :return: unique documents
        """
        # Build requests - We are building requests for individual hash actors
        request = [[] for _ in range(len(self.hashes))]

        for h in hd.keys():
            request[TransformUtils.str_to_int(h) % len(self.hashes)].append(h)

        # Submit requests to appropriate hash actors
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.hashes[i].get_unique.remote(req))
            i = i + 1
        # Process replies
        unique = []
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            # Populate uniques for completed replies
            for red in ray.get(ready):
                for uh in red:
                    unique.append(hd[uh])
            # Continue waiting for not completed replies
            remote_replies = not_ready
        return unique


class EdedupRuntime(DefaultTableTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            doc_column - name of the doc column
            hash_cpu - cpus per hash instance
            num_hashes - number of hashes
        """
        super().__init__(params)
        self.filters = []

    def set_environment(self, data_access: DataAccess) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access - data access class
        :return: dictionary of filter init params
        """
        # create hashes
        self.filters = RayUtils.create_actors(
            clazz=HashFilter,
            params={},
            actor_options={"num_cpus": self.params.get("hash_cpu", 0.5)},
            n_actors=self.params.get("num_hashes", 1),
        )
        return {"hashes": self.filters} | self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        # Get filters stats
        sum_hash = 0
        sum_hash_mem = 0
        remote_replies = [f.get_hash_size.remote() for f in self.filters]
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            for r in ready:
                h_size, h_memory = ray.get(r)
                sum_hash = sum_hash + h_size
                sum_hash_mem = sum_hash_mem + h_memory
            remote_replies = not_ready
        dedup_prst = 100 * (1.0 - stats.get("result_documents", 1) / stats.get("source_documents", 0))
        return {"number of hashes": sum_hash, "hash memory, GB": sum_hash_mem, "de duplication %": dedup_prst} | stats


class EdedupTableTransformConfiguration(DefaultTableTransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="ededup", runtime_class=EdedupRuntime, transform_class=EdedupTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        parser.add_argument("--hash_cpu", type=float, default=0.5, help="number of CPUs per hash")
        parser.add_argument("--num_hashes", type=int, default=0, help="number of hash actors to use")
        parser.add_argument("--doc_column", type=str, default="contents", help="key for accessing data")

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.num_hashes <= 0:
            print(f"Number of hashes should be greater then zero, provided {args.num_hashes}")
            return False
        self.params["doc_column"] = args.doc_column
        self.params["hash_cpu"] = args.hash_cpu
        self.params["num_hashes"] = args.num_hashes
        print(f"exact dedup params are {self.params}")
        return True
