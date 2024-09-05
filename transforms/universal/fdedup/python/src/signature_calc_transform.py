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
import time
import uuid
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, List

import mmh3
import numpy as np
import polars as pl
import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider
from Murmur_MH import Murmur_MH
from scipy.integrate import quad as integrate


short_name = "minhash"
cli_prefix = f"{short_name}_"
sleep_key = "sleep_sec"
pwd_key = "pwd"
sleep_cli_param = f"{cli_prefix}{sleep_key}"
pwd_cli_param = f"{cli_prefix}{pwd_key}"


class SignatureCalculationTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, NOOPTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of NOOPTransformConfiguration class
        super().__init__(config)
        self.sleep = config.get("sleep_sec", 1)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        self.logger.info(f"Transforming one table with {len(table)} rows")

        print("----minhash---")
        print(file_name)
        extracted_file_name = Path(file_name).stem
        print(extracted_file_name)
        # Generate a UUID
        unique_id = uuid.uuid4()

        # Concatenate the UUID with the file name
        output_file_name = f"{extracted_file_name}_{unique_id}"

        base_path = "/Users/nelson/workspace/Research/DataPreprocessing/ibm/active/data-prep-kit/transforms/universal/noop/python/test-data/outputs2/"
        num_segments = 2
        # load the data from pyarrow table
        polars_df = pl.from_arrow(table)

        # read the target columns
        polars_df = polars_df.select("contents", "int_id_column")

        # instantiate once on driver so that every worker will use the same hash functions
        mm_min_hash = Murmur_MH(num_perm=64, seed=42)

        # generate minhash values
        minhashed = polars_df.map_rows(
            lambda text: mm_min_hash.minhash2_nosalt(*self._generate_word_shingles(text, window_size=8))
        )

        # Define new column names and schema
        # Assuming document_id_column is mapped to column_1
        document_id_column = "document_id"
        data_column = "data"

        # Rename columns
        df_renamed = minhashed.rename(
            {
                "column_0": f"{data_column}.minhashes",
                "column_1": f"{data_column}.document_length",
                "column_2": document_id_column,
            }
        )

        # Create a nested structure as a new DataFrame
        df_transformed = df_renamed.with_columns(
            [
                # Create the 'data' column as a Struct with minhashes and document_length
                pl.struct([pl.col(f"{data_column}.minhashes"), pl.col(f"{data_column}.document_length")]).alias(
                    data_column
                )
            ]
        ).drop([f"{data_column}.minhashes", f"{data_column}.document_length"])

        # Rename the 'data' column to the final schema
        minhashed_data_frame = df_transformed.rename({data_column: "data"})

        minhashed_data_path = os.path.join(base_path, "minhashes")
        os.makedirs(minhashed_data_path, exist_ok=True)  # Create the directory if it does not exist
        self.write_data(minhashed_data_path, minhashed_data_frame, f"{output_file_name}", "parquet")

        # Start bands calculation
        minhashlsh_num_bands, minhashlsh_length_band = self._optimal_minhashlsh_param(
            threshold=0.8,
            num_perm=64,
            false_positive_weight=0.5,
            false_negative_weight=0.5,
        )

        # Convert Polars DataFrame to Pandas
        minhashed_data_frame_pandas = minhashed_data_frame.to_pandas()

        minhashed_data_frame_pandas_processed = self.process_rows_into_bands(
            minhashed_data_frame_pandas, minhashlsh_num_bands, minhashlsh_length_band
        )

        # Define the schema
        schema = pl.Schema(
            {
                "band_hash": pl.Int64,
                "band_index": pl.Int32,
                "document_data": pl.Struct(
                    {"int_id_column": pl.Int64, "minhashes": pl.List(pl.Int32), "document_length": pl.Int32}
                ),
            }
        )

        bands_dataframe = pl.DataFrame(minhashed_data_frame_pandas_processed, schema=schema)
        bands_data_path = os.path.join(base_path, "bands")

        segment_bounds_list = []
        upper_bound = np.iinfo(np.uint64).max
        for segment_index in range(num_segments // 2):
            segment_bounds_list.append(np.uint64(segment_index * (upper_bound // num_segments)))
        segment_bounds_list.append(np.uint64(upper_bound // 2))
        segment_bounds_list.append(np.uint64(upper_bound // 2 + 1))
        for segment_index in range(num_segments // 2 + 1, num_segments):
            segment_bounds_list.append(np.uint64(segment_index * (upper_bound // num_segments)))
        segment_bounds_list.append(np.uint64(upper_bound))
        segment_bounds = np.array(segment_bounds_list, dtype=np.uint64)
        segment_bounds = np.array(segment_bounds, dtype=np.int64).tolist()
        tables = []
        paths = []
        for band_ix in range(2):

            # Filtering and dropping the column
            band_df = bands_dataframe.filter(pl.col("band_index") == band_ix).drop("band_index")

            segment_index = 0
            segment_band_df = band_df.filter(
                (pl.col("band_hash") >= segment_bounds[segment_index])
                & (pl.col("band_hash") <= segment_bounds[segment_index + 1])
            )
            segment_output_path = os.path.join(bands_data_path, f"band={band_ix}", f"segment={segment_index}")
            os.makedirs(segment_output_path, exist_ok=True)
            tables.append(segment_band_df.to_arrow())
            paths.append(
                f"bands/band={band_ix}/segment={segment_index}/",
            )
            # self.write_data(segment_output_path, segment_band_df, f"{output_file_name}_band_{band_ix}_segment_{segment_index}", 'parquet')
        metadata = {"nfiles": 1, "nrows": len(table), "paths": paths}
        return tables, metadata

        #
        # for segment_index in range(1, num_segments // 2):
        #     segment_band_df = band_df.filter(
        #         (pl.col("band_hash") > segment_bounds[segment_index])
        #         & (pl.col("band_hash") <= segment_bounds[segment_index + 1])
        #     )
        #
        #     segment_output_path = os.path.join(bands_data_path, f"band={band_ix}", f"segment={segment_index}")
        #     os.makedirs(segment_output_path, exist_ok=True)
        #     self.write_data(segment_output_path, segment_band_df, f"{output_file_name}_band_{band_ix}_segment_{segment_index}", 'parquet')
        #
        # for segment_index in range(num_segments // 2, num_segments - 1):
        #     segment_band_df = band_df.filter(
        #         (pl.col("band_hash") > segment_bounds[segment_index])
        #         & (pl.col("band_hash") <= segment_bounds[segment_index + 1])
        #     )
        #
        #     segment_output_path = os.path.join(bands_data_path, f"band={band_ix}", f"segment={segment_index}")
        #     os.makedirs(segment_output_path, exist_ok=True)
        #     self.write_data(segment_output_path, segment_band_df,
        #                     f"{output_file_name}_band_{band_ix}_segment_{segment_index}", 'parquet')
        #
        # segment_index = num_segments - 1
        #
        # segment_band_df = band_df.filter(
        #     (pl.col("band_hash") >= segment_bounds[segment_index + 1])
        #     & (pl.col("band_hash") <= segment_bounds[segment_index + 2])
        # )
        #
        # segment_output_path = os.path.join(bands_data_path, f"band={band_ix}", f"segment={segment_index}")
        # os.makedirs(segment_output_path, exist_ok=True)
        # self.write_data(segment_output_path, segment_band_df,
        #                 f"{output_file_name}_band_{band_ix}_segment_{segment_index}", 'parquet')
        #
        #

        # if self.sleep is not None:
        #     self.logger.info(f"Sleep for {self.sleep} seconds")
        #     time.sleep(self.sleep)
        #     self.logger.info("Sleep completed - continue")
        # # Add some sample metadata.
        # self.logger.debug(f"Transformed one table with {len(table)} rows")
        # metadata = {"nfiles": 1, "nrows": len(table), "path":"bands/band=0/segment=0/"}
        # #/output/minhash
        # #/output/bands/band=0/segment=0
        # return [table], metadata

    # define shingles generation function
    def _generate_word_shingles(self, text: str, window_size: int = 5, delimiter: str = " ") -> tuple[list, int, int]:
        words = text[0].split()
        document_id = text[1]
        doc_len = len(text[0])
        word_count = len(words)
        k_shingles = []
        for i in range(0, max(1, word_count - window_size + 1)):
            k_shingles.append(delimiter.join(words[i : i + window_size]))
        return k_shingles, doc_len, document_id

    def write_data(self, band_output_path, df, file_name, data_type="parquet"):
        print("=====write_data====")
        print(band_output_path)
        file_path = os.path.join(band_output_path, f"{file_name}.{data_type}")
        if data_type == "parquet":
            df.write_parquet(file_path)
        elif data_type == "csv":
            df.write_csv(file_path)
        elif data_type == "json":
            df.write_json(file_path)
        else:
            raise ValueError(f"Unsupported data_type: {data_type}")

    def emit_bands(self, int_id_column: str, minhashes: np.array, doc_length: int, b: int, r: int, seed: int = 42):
        num_minhashes = len(minhashes)
        assert b * r <= num_minhashes, f"b*r must be <= num minhashes, was b={b}, r={r}, num_minhashes={num_minhashes}"
        results = []
        for band_index in range(b):
            band_hash = mmh3.hash64(minhashes[band_index * r : (band_index + 1) * r], seed=seed, signed=True)[0]
            results.append(
                (
                    band_hash,
                    band_index,
                    {
                        "int_id_column": int_id_column,
                        "minhashes": minhashes.astype(np.int32).tolist(),
                        "document_length": doc_length,
                    },
                )
            )
        return results

    # Apply the function using Pandas
    def process_rows_into_bands(self, df, minhashlsh_num_bands, minhashlsh_length_band):
        result = []
        for _, row in df.iterrows():
            bands = self.emit_bands(
                row["document_id"],
                np.array(row["data"]["data.minhashes"]),
                row["data"]["data.document_length"],
                minhashlsh_num_bands,
                minhashlsh_length_band,
            )
            for band in bands:
                result.append(band)

        return result

    def sort_clusters(self, minhash_cluster_df: pl.DataFrame) -> pl.DataFrame:
        sorted_df = minhash_cluster_df.sort(by=["cluster_length"], descending=True)
        return sorted_df

    def jaccard_distance_calculation(self, row: List[pl.Series]) -> List[pl.Series]:
        # Process row and return a new list of Series or a new row
        threshold = 0.8
        docs_to_remove = []

        # Extracting int_id_column values into a list
        doc_list = [item["int_id_column"] for item in row[1]]

        # Creating a dictionary with int_id_column as key and minhashes as value
        doc_minhashes = {item["int_id_column"]: item["minhashes"] for item in row[1]}

        # this is the document we are going to keep
        first_doc = doc_list[0]
        first_mh = doc_minhashes[first_doc]

        for int_id_column in doc_list[1:]:
            doc_mh = doc_minhashes[int_id_column]
            distance = Murmur_MH.jaccard(np.array(first_mh), np.array(doc_mh))
            if distance >= threshold:
                docs_to_remove.append(int_id_column)
        docs_to_remove = list(set(docs_to_remove))

        return (first_doc, docs_to_remove, len(docs_to_remove))

    def process_bands(self, df: pl.DataFrame) -> pl.DataFrame:
        # Apply row processing
        processed_rows = df.map_rows(lambda row: self.jaccard_distance_calculation(row))
        return processed_rows

    def _optimal_minhashlsh_param(
        self,
        threshold: float = 0.8,
        num_perm: int = 128,
        false_positive_weight: float = 0.5,
        false_negative_weight: float = 0.5,
    ):
        """
        Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
        of probabilities of false positive and false negative.
        :param threshold: desired similarity threshold
        :param num_perm: number of permutations
        :param false_positive_weight: importance of avoiding false positive results
        :param false_negative_weight: importance of avoiding false negative results
        :return: a tuple (optimal number of bands, optimal number of rows)
        """

        def _false_positive_probability(threshold, b, r):
            _probability = lambda s: 1 - (1 - s ** float(r)) ** float(b)
            a, err = integrate(_probability, 0.0, threshold)
            return a

        def _false_negative_probability(threshold, b, r):
            _probability = lambda s: 1 - (1 - (1 - s ** float(r)) ** float(b))
            a, err = integrate(_probability, threshold, 1.0)
            return a

        min_error = float("inf")
        opt = (0, 0)
        for b in range(1, num_perm + 1):
            max_r = int(num_perm / b)
            for r in range(1, max_r + 1):
                fp = _false_positive_probability(threshold, b, r)
                fn = _false_negative_probability(threshold, b, r)
                error = fp * false_positive_weight + fn * false_negative_weight
                if error < min_error:
                    min_error = error
                    opt = (b, r)
        return opt


class SignatureCalculationTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=SignatureCalculationTransform,
            remove_from_metadata=[pwd_key],
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{sleep_cli_param}",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )
        # An example of a command line option that we don't want included
        # in the metadata collected by the Ray orchestrator
        # See below for remove_from_metadata addition so that it is not reported.
        parser.add_argument(
            f"--{pwd_cli_param}",
            type=str,
            default="nothing",
            help="A dummy password which should be filtered out of the metadata",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(sleep_key) < 0:
            print(f"Parameter noop_sleep_sec should be non-negative. you specified {args.noop_sleep_sec}")
            return False

        self.params = self.params | captured
        self.logger.info(f"noop parameters are : {self.params}")
        return True
