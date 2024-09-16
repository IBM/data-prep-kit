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
import os
from argparse import ArgumentParser, Namespace
from typing import Any, List, Tuple

import numpy as np
import polars as pl
import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger
from Murmur_MH import Murmur_MH


short_name = "cluster"
cli_prefix = f"{short_name}_"

# configuration keys
jaccard_similarity_threshold_key = "jaccard_similarity_threshold"
""" This key holds the Jaccard similarity threshold above which two documents are duplicates"""

# command line arguments
jaccard_similarity_threshold_cli_param = f"{cli_prefix}{jaccard_similarity_threshold_key}"
""" Jaccard similarity threshold above which two documents are duplicates"""

captured_arg_keys = [
    jaccard_similarity_threshold_key,
]

# defaults
jaccard_similarity_threshold_default = 0.8
""" Default Jaccard similarity threshold above which two documents are duplicates"""


class ClusterAnalysisTransform(AbstractTableTransform):
    """
    This is the second transform of the fuzzy dedup pipeline. It runs in parallel:
    for each band, the hashing interval is divided into segments. A cluster analysis
    uses as input all the parquet files from segment of a band. The `bands` output
    of the signature calculation, the first transform in the fuzzy dedup pipeline
    contains all the data for a given segment s of a specific band b in the
    subfolder `bands/band=b/segment=s`.
    The transform loads all the parquet files in the `bands/band=b/segment=s`
    subfolder. Each one of these parquet files has two columns: the `band_hash`
    and a `data` structure, which includes the `document_id`, the `minhashes` and
    the `document_size` fields. Once all the files have been loaded in a single
    dataframe, a `group_by` operation on the `band_hash` field is performed in
    that dataframe. All the documents that have the same band_hash are grouped
    in a cluster. Subsequently, the documents of each cluster are sorted in
    descending order according to their size, and a Jaccard similarity is
    calculated between the cluster documents. The documents for which the Jaccard
    similarity is above the `jaccard_similarity_threshold` remain in the cluster,
    the others are removed from the cluster. Finally, from each cluster that has
    more than one document after running the Jaccard similarity, we select a doc
    to keep (the largest size document), and mark the other documents as
    duplicates. The resulting clusters are saved in a file for further analysis.

    Args:
        jaccard_similarity_threshold: Jaccard similarity threshold above which two documents are duplicates
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments
        defined by the companion runtime, ClusterAnalysisTransformRuntime.
        """
        super().__init__(config)
        self.jaccard_similarity_threshold = config.get(
            jaccard_similarity_threshold_key, jaccard_similarity_threshold_default
        )
        self.logger = get_logger(__name__)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        bands_dataframe = pl.from_arrow(table)
        docs2remove_list = []
        # clustering
        bands_dataframe_groups = bands_dataframe.group_by("band_hash").agg("document_data")
        bands_dataframe_cluster = bands_dataframe_groups.with_columns(
            cluster_length=pl.col("document_data").list.len()
        ).filter(pl.col("cluster_length") > 1)
        self.logger.info(f"file_name = {file_name}")
        self.logger.info(f"After GroupBy: {len(bands_dataframe_cluster)} clusters")
        bands_dataframe_response = self.process_bands(bands_dataframe_cluster)

        filtered_doc2remove_dataframe = bands_dataframe_response.filter(pl.col("docs_to_remove_length") > 0)
        filtered_doc2remove_dataframe = filtered_doc2remove_dataframe.drop("docs_to_remove_length")
        self.logger.info(f"After Jaccard: {len(filtered_doc2remove_dataframe)} clusters")

        # Explode the 'minhashes' column
        doc2remove_exploded_dataframe = filtered_doc2remove_dataframe.explode("docs_to_remove")
        table = doc2remove_exploded_dataframe.to_arrow()
        self.logger.info(f"{len(doc2remove_exploded_dataframe)} documents marked to remove")
        metadata = {"nfiles": doc2remove_exploded_dataframe.count(), "nrows": len(table)}
        return [table], metadata

    def process_bands(self, df: pl.DataFrame) -> pl.DataFrame:
        # Define the schema with specific data types
        schema = {"first_doc": pl.Int64, "docs_to_remove": pl.List(pl.Int64), "docs_to_remove_length": pl.Int64}
        doc_ids_lists = []
        docs_to_remove_lists = []
        len_of_docs2remove_lists = []
        for row in df.iter_rows(named=True):
            doc_ids_list, docs_to_remove_list, len_of_docs2remove_list = self.jaccard_distance_calculation(row)
            doc_ids_lists += doc_ids_list
            docs_to_remove_lists += docs_to_remove_list
            len_of_docs2remove_lists += len_of_docs2remove_list
        processed_rows = pl.DataFrame(
            {
                "first_doc": doc_ids_lists,
                "docs_to_remove": docs_to_remove_lists,
                "docs_to_remove_length": len_of_docs2remove_lists,
            },
            schema=schema,
        )
        return processed_rows

    def jaccard_distance_calculation(self, row: List[pl.Series]) -> list[list]:
        # Process row and return a new list of Series or a new row
        threshold = self.jaccard_similarity_threshold
        doc_ids_list = []
        docs_to_remove_list = []
        len_of_docs2remove_list = []
        # sort documents
        document_data = row["document_data"]

        # Sort the list by 'document_length'
        sorted_document_data = sorted(document_data, key=lambda x: (-x["document_length"], x["int_id_column"]))

        # Extracting int_id_column values into a list
        doc_list = list(set([item["int_id_column"] for item in sorted_document_data]))

        # Creating a dictionary with int_id_column as key and minhashes as value
        doc_minhashes = {item["int_id_column"]: item["minhashes"] for item in sorted_document_data}

        while len(doc_list) > 1:
            docs_to_remove = []
            new_doc_list = []
            # this is the document we are going to keep
            first_doc = doc_list[0]
            first_mh = doc_minhashes[first_doc]
            for int_id_column in doc_list[1:]:
                doc_mh = doc_minhashes[int_id_column]
                distance = Murmur_MH.jaccard(np.array(first_mh), np.array(doc_mh))
                if distance >= threshold:
                    docs_to_remove.append(int_id_column)
                else:
                    new_doc_list.append(int_id_column)
            if len(docs_to_remove) > 0:
                docs_to_remove = list(set(docs_to_remove))
                doc_ids_list.append(first_doc)
                docs_to_remove_list.append(docs_to_remove)
                len_of_docs2remove_list.append(len(docs_to_remove))
            doc_list = new_doc_list

        return doc_ids_list, docs_to_remove_list, len_of_docs2remove_list


class ClusterAnalysisTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=ClusterAnalysisTransform,
            remove_from_metadata=[],
        )
        self.logger = get_logger(__name__, level="INFO")

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{jaccard_similarity_threshold_cli_param}",
            type=float,
            default=jaccard_similarity_threshold_default,
            help="Jaccard similarity threshold above which two documents are duplicates",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"{short_name} parameters are : {self.params}")
        return True
