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
import io
import os
import re
from argparse import ArgumentParser, Namespace
from typing import Any, List, Tuple

import numpy as np
import polars as pl
import pyarrow as pa
from data_processing.transform import AbstractFolderTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, get_logger
from Murmur_MH import Murmur_MH


short_name = "cluster"
cli_prefix = f"{short_name}_"

# configuration keys
num_bands_key = "num_bands"
""" This key holds the number of bands used in the banding technique"""
num_segments_key = "num_segments"
""" This key holds the number of segments dividing the hashing space for each band"""
jaccard_similarity_threshold_key = "jaccard_similarity_threshold"
""" This key holds the Jaccard similarity threshold above which two documents are duplicates"""

# command line arguments
num_bands_cli_param = f"{cli_prefix}{num_bands_key}"
""" The number of bands used in the banding technique"""
jaccard_similarity_threshold_cli_param = f"{cli_prefix}{jaccard_similarity_threshold_key}"
""" Jaccard similarity threshold above which two documents are duplicates"""
num_segments_cli_param = f"{cli_prefix}{num_segments_key}"
""" The number of segments dividing the hashing space for each band"""

captured_arg_keys = [
    num_bands_key,
    num_segments_key,
    jaccard_similarity_threshold_key,
]

# defaults
num_bands_default = 14
""" Default number of bands used in the banding technique (from FineWeb https://arxiv.org/pdf/2406.17557)"""
jaccard_similarity_threshold_default = 0.75
""" Default Jaccard similarity threshold (from FineWeb https://arxiv.org/pdf/2406.17557)"""
num_segments_default = 1
""" Default number of segments dividing the hashing space for each band"""


class ClusterAnalysisTransform(AbstractFolderTransform):
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
        num_bands: number of bands used in the banding technique
        jaccard_similarity_threshold: Jaccard similarity threshold above which two documents are duplicates
        num_segments: the number of segments dividing the hashing space for each band
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments
        defined by the companion runtime, ClusterAnalysisTransformRuntime.
        """
        super().__init__(config)
        self.num_bands = config.get(num_bands_key, num_bands_default)
        self.num_segments = config.get(num_segments_key, num_segments_default)
        self.jaccard_similarity_threshold = config.get(
            jaccard_similarity_threshold_key, jaccard_similarity_threshold_default
        )
        self.data_access = config.get("data_access")
        self.logger = get_logger(__name__)

    def transform(self, folder_name: str) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        self.logger.info(f"Cluster analysis for folder {folder_name}")
        metadata = {}
        input_folder = self.sanitize_folder_name(os.path.join(self.data_access.input_folder, folder_name))
        files, retries = self.data_access.get_folder_files(
            path=input_folder,
            extensions=[".parquet"],
            return_data=True,
        )
        if retries > 0:
            metadata |= {"data_access_retries": retries}
        match = re.match(r"^band=(\d+)/segment=(\d+)$", folder_name)
        if match:
            band = int(match.group(1))
            segment = int(match.group(2))
        else:
            raise ValueError(f"Wrong folder_name {folder_name}, should be band=b/segment=s")
        output_folder = self.sanitize_folder_name(self.data_access.output_folder)
        output_path = os.path.join(output_folder, f"band_{band}_segment_{segment}.parquet")

        # consolidate into a single data frame band hashes computed by workers
        band_segment_dataframe, consolidation_stats = self.consolidate_band_segment_files(files)
        metadata |= consolidation_stats
        # cluster grouping by band hashes
        cluster_dataframe, cluster_stats = self.get_clusters(band_segment_dataframe)
        metadata |= cluster_stats
        # cluster analysis using jaccard similarity
        jaccard_cluster_dataframe, jaccard_stats = self.analyze_clusters(cluster_dataframe)
        metadata |= jaccard_stats
        # Generate the docs_to_remove dataframe
        docs_to_remove_dataframe = jaccard_cluster_dataframe.explode("docs_to_remove")
        output_data = TransformUtils.convert_arrow_to_binary(docs_to_remove_dataframe.to_arrow())
        self.logger.info(f"{len(docs_to_remove_dataframe)} documents marked to remove")
        metadata |= {"num_duplicate_documents": len(docs_to_remove_dataframe)}
        return [(output_data, output_path)], metadata

    def sanitize_folder_name(self, folder_name: str) -> str:
        if "://" in folder_name:
            _, folder_name = folder_name.split("://")
        if folder_name[-1] != "/":
            folder_name = f"{folder_name}/"
        return folder_name

    def consolidate_band_segment_files(self, files: dict[str, bytes]) -> tuple[pl.DataFrame, dict[str, Any]]:
        band_segment_dataframe = pl.DataFrame()
        total_input_rows = 0
        for fname, contents in files.items():
            df = pl.read_parquet(io.BytesIO(contents))
            total_input_rows += len(df)
            self.logger.debug(f"{fname} has {len(df)} rows")
            band_segment_dataframe = band_segment_dataframe.vstack(df)

        consolidation_stats = {
            "input_files": len(files),
            "input_bytes": sum(len(v) for v in files.values()),
            "input_rows": total_input_rows,
            "consolidated_files": 1,
            "consolidated_bytes": band_segment_dataframe.to_arrow().nbytes,
            "consolidated_rows": len(band_segment_dataframe),
        }
        return band_segment_dataframe, consolidation_stats

    def get_clusters(self, band_segment_dataframe: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        groupby_dataframe = band_segment_dataframe.group_by("band_hash").agg("document_data")
        cluster_dataframe = groupby_dataframe.with_columns(cluster_length=pl.col("document_data").list.len()).filter(
            pl.col("cluster_length") > 1
        )
        # self.logger.info(f"file_name = {file_name}")
        num_clusters = len(cluster_dataframe)
        if num_clusters > 0:
            sum_cdocs = cluster_dataframe.select(pl.sum("cluster_length")).item()
            max_cdocs = cluster_dataframe.select(pl.max("cluster_length")).item()
            min_cdocs = cluster_dataframe.select(pl.min("cluster_length")).item()
            avg_cdocs = cluster_dataframe.select(pl.mean("cluster_length")).item()
        else:
            sum_cdocs = 0
            max_cdocs = 0
            min_cdocs = 0
            avg_cdocs = 0
        self.logger.info(f"After GroupBy: {num_clusters} clusters with {sum_cdocs} total docs")
        self.logger.info(f" max/min/avg docs per cluster: {max_cdocs}/{min_cdocs}/{avg_cdocs:.2f}")
        cluster_stats = {
            "groupby_clusters": num_clusters,
            "cluster_duplicate_docs": sum_cdocs,
        }
        return cluster_dataframe, cluster_stats

    def analyze_clusters(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
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
        jaccard_cluster_dataframe = pl.DataFrame(
            {
                "first_doc": doc_ids_lists,
                "docs_to_remove": docs_to_remove_lists,
                "docs_to_remove_length": len_of_docs2remove_lists,
            },
            schema=schema,
        )
        filtered_jaccard_dataframe = jaccard_cluster_dataframe.filter(pl.col("docs_to_remove_length") > 0)
        num_clusters = len(filtered_jaccard_dataframe)
        if num_clusters > 0:
            sum_cdocs = filtered_jaccard_dataframe.select(pl.sum("docs_to_remove_length")).item()
            max_cdocs = filtered_jaccard_dataframe.select(pl.max("docs_to_remove_length")).item()
            min_cdocs = filtered_jaccard_dataframe.select(pl.min("docs_to_remove_length")).item()
            avg_cdocs = filtered_jaccard_dataframe.select(pl.mean("docs_to_remove_length")).item()
        else:
            sum_cdocs = 0
            max_cdocs = 0
            min_cdocs = 0
            avg_cdocs = 0
        self.logger.info(f"After Jaccard: {num_clusters} clusters with {sum_cdocs} total docs")
        self.logger.info(f" max/min/avg docs per cluster: {max_cdocs}/{min_cdocs}/{avg_cdocs:.2f}")
        jaccard_stats = {
            "jaccard_clusters": num_clusters,
            "jaccard_duplicate_docs": sum_cdocs,
        }
        return filtered_jaccard_dataframe, jaccard_stats

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
        doc_list = [item["int_id_column"] for item in sorted_document_data]

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
        parser.add_argument(
            f"--{num_bands_cli_param}",
            type=int,
            default=num_bands_default,
            help="The number of bands used in the banding technique",
        )
        parser.add_argument(
            f"--{num_segments_cli_param}",
            type=int,
            default=num_segments_default,
            help="The number of segments dividing the hashing space for each band",
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
