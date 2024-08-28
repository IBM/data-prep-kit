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

from argparse import ArgumentParser, Namespace
from typing import Any

import mmh3
import numpy as np
import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import (
    RANDOM_SEED,
    CLIArgumentProvider,
    TransformUtils,
)
from data_processing.utils import UnrecoverableException

# performance
REQUEST_LEN = 4096

# configuration parameters
short_name = "fdedup_preprocessor"
preprocessor_cli_prefix = f"{short_name}_"
doc_column_name_key = "doc_column"
int_column_name_key = "doc_id_column"
delimiters_key = "delimiter"
num_permutations_key = "num_permutations"
threshold_key = "threshold"
shingles_size_key = "shingles_size"
minhash_snapshot_directory_key = "minhash_snapshot_directory"
buckets_snapshot_directory_key = "buckets_snapshot_directory"
# internal parameters
mn_min_hash_key = "mn_min_hash"
num_bands_key = "num_bands"
length_band_key = "length_band"
buckets_cache_key = "buckets_cache"
minhashes_cache_key = "minhashes_cache"

preprocessor_doc_column_name_cli_param = f"{preprocessor_cli_prefix}{doc_column_name_key}"
preprocessor_int_column_name_cli_param = f"{preprocessor_cli_prefix}{int_column_name_key}"
delimiters_cli_param = f"{preprocessor_cli_prefix}{delimiters_key}"
preprocessor_num_permutations_cli_param = f"{preprocessor_cli_prefix}{num_permutations_key}"
preprocessor_threshold_cli_param = f"{preprocessor_cli_prefix}{threshold_key}"
shingles_size_cli_param = f"{preprocessor_cli_prefix}{shingles_size_key}"
preprocessor_minhash_snapshot_directory_cli_param = f"{preprocessor_cli_prefix}{minhash_snapshot_directory_key}"
preprocessor_buckets_snapshot_directory_cli_param = f"{preprocessor_cli_prefix}{buckets_snapshot_directory_key}"


class FdedupPreprocessorTransformBase(AbstractTableTransform):
    """
    Implements fuzzy dedup data preprocessor (building tables, minhashes and buckets).
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters, with the following keys
            doc_column - name of doc column
            doc_id_int_column - name of int doc id column
            word_shingle_size - word shingle size
            mn_min_hash - MurmurMH class
            num_bands - number of bands
            length_band band length
            delimiter - delimiter
        """
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        super().__init__(config)
        self.doc_column = config.get(doc_column_name_key, "contents")
        self.doc_id_column = config.get(int_column_name_key, "int_document_id")
        self.word_shingle_size = config.get(shingles_size_key, 5)
        self.delimiter = config.get(delimiters_key, " ")
        self.mn_min_hash = config.get(mn_min_hash_key, None)
        if self.mn_min_hash is None:
            raise UnrecoverableException("Minhash class is not provided")
        self.num_bands = config.get(num_bands_key, 1)
        self.length_band = config.get(length_band_key, 1)

    def _generate_minhashes(self, shingles: list[str]) -> np.array:
        """
        Generate minhashes
        :param shingles:
        :return: generated minhashes
        """
        min_hashes = self.mn_min_hash.minhash(len(shingles), shingles)
        num_min_hashes = len(min_hashes)
        assert self.num_bands * self.length_band <= num_min_hashes, (
            f"num_bans*band_len must be <= num min hashes, was num_bands={self.num_bands}, "
            f"bands_len={self.length_band}, num_min hashes={num_min_hashes}"
        )
        return min_hashes

    def _generate_buckets(self, min_hashes: np.array) -> list[int]:
        """
        Generate buckets
        :param min_hashes: array of minhashes
        :return:
        """
        return [
            mmh3.hash64(min_hashes[i * self.length_band: (i + 1) * self.length_band], seed=RANDOM_SEED, signed=False)[
                0
            ]
            for i in range(self.num_bands)
        ]

    def _submit_buckets_minhashes(
            self, buckets: dict[int, list[int]], minhashes: list[tuple[int, int, np.array]]
    ) -> None:
        """
        Submit buckets to hash
        :param buckets: buckets
        :param minhashes: minhashes
        :return: None
        """
        raise NotImplementedError

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Preprocessing table content.
        :param table: table
        :param file_name - name of currently processed file
        :return: resulting table, statistics
        """
        from fdedup.utils import compute_shingles

        def _flush_to_cache(limit: int) -> None:
            """
            flushing buckets and minhashes to dedicated actors
            :param limit: number of buckets to flush
            :return: None
            """
            if len(buckets) >= limit:  # time to submit
                nonlocal num_buckets
                nonlocal num_minhashes
                self.logger.debug(f"saving {len(buckets)} buckets and {len(minhashes)} minhashes")
                self._submit_buckets_minhashes(buckets, minhashes)
                num_buckets = num_buckets + len(buckets)
                num_minhashes = num_minhashes + len(minhashes)
                buckets.clear()
                minhashes.clear()

        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column, self.doc_id_column])
        self.logger.debug(f"Preprocessing a new table {file_name}")
        # Inner variables
        buckets = {}
        minhashes = []
        num_buckets = 0
        num_minhashes = 0
        docs = table[self.doc_column]
        doc_ids = table[self.doc_id_column]
        # for every document/its integer id
        for n in range(table.num_rows):
            doc = docs[n].as_py()
            doc_id = doc_ids[n].as_py()
            shingles = compute_shingles(txt=doc, word_shingle_size=self.word_shingle_size, delimiter=self.delimiter)
            if len(shingles) > 0:
                mh = self._generate_minhashes(shingles)
                minhashes.append((doc_id, len(doc), mh))
                candidates = self._generate_buckets(mh)

                for b_hash in candidates:
                    bucket_array = buckets.get(b_hash)
                    if bucket_array is None:
                        buckets[b_hash] = [doc_id]
                    else:
                        bucket_array.append(doc_id)
                _flush_to_cache(REQUEST_LEN)
        _flush_to_cache(0)
        # peg stats
        self.logger.debug(f"for table {file_name} generated {num_buckets} buckets and {num_minhashes} minhashes")
        stats = {"generated buckets": num_buckets, "generated minhashes": num_minhashes}
        return [], stats


class FdedupPreprocessorTransformConfigurationBase(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self, transform_class: type[AbstractTableTransform]):
        super().__init__(
            name=short_name,
            transform_class=transform_class,
        )
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        parser.add_argument(
            f"--{preprocessor_doc_column_name_cli_param}",
            type=str,
            default="contents",
            help="document column name")
        parser.add_argument(
            f"--{preprocessor_int_column_name_cli_param}",
            type=str,
            default="int_document_id",
            help="integer document id column name"
        )
        parser.add_argument(
            f"--{preprocessor_num_permutations_cli_param}",
            type=int,
            default=64,
            help="number of permutations")
        parser.add_argument(
            f"--{preprocessor_threshold_cli_param}",
            type=float,
            default=0.8,
            help="threshold"
        )
        parser.add_argument(
            f"--{shingles_size_cli_param}",
            type=int,
            default=5,
            help="number of words in shingle")
        parser.add_argument(
            f"--{delimiters_cli_param}",
            type=str,
            default=" ",
            help="delimiter for splitting document"
        )
        parser.add_argument(
            f"--{preprocessor_minhash_snapshot_directory_cli_param}",
            type=str,
            default=None,
            help="minhash snapshot directory key",
        )
        parser.add_argument(
            f"--{preprocessor_buckets_snapshot_directory_cli_param}",
            type=str,
            default=None,
            help="buckets snapshot directory key",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, preprocessor_cli_prefix, False)
        self.params = self.params | captured
        return True
