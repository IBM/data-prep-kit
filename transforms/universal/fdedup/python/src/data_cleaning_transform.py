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
from data_processing.utils import CLIArgumentProvider


short_name = "cluster"
cli_prefix = f"{short_name}_"


class DataCleaningTransform(AbstractTableTransform):
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

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        bands_dataframe = pl.from_arrow(table)
        docs2remove_list = []

        # read all files in a given segment
        # TODO: NB: Please update the path
        path_to_consolidate_docs_to_remove = "~/data-prep-kit/transforms/universal/fdedup/python/output_second/docs_to_remove_consolidated/docs_to_remove_consolidated.parquet"
        doc2remove_polars_df = pl.read_parquet(path_to_consolidate_docs_to_remove)
        # Convert the 'exploded_minhashes' column to a list of integers
        docs_to_remove_list = doc2remove_polars_df.to_series().to_list()
        docs2remove_list.extend(docs_to_remove_list)
        docs2remove_list = list(set(docs2remove_list))
        # Filter out rows where the 'id' column is in the given list
        filtered_df = bands_dataframe.filter(~pl.col("int_id_column").is_in(docs2remove_list))
        table = filtered_df.to_arrow()
        metadata = {"nfiles": filtered_df.count(), "nrows": len(table)}
        return [table], metadata


class DataCleaningTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=DataCleaningTransform,
            remove_from_metadata=[],
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__, level="INFO")

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        # parser.add_argument(
        #     f"--{document_id_column_cli_param}",
        #     type=str,
        #     default=document_id_column_default,
        #     help="name of the column storing the unique ID assigned to each document",
        # )

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
