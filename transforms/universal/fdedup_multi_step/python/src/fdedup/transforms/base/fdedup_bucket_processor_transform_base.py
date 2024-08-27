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

from argparse import Namespace, ArgumentParser
import pickle
from typing import Any

from data_processing.transform import AbstractBinaryTransform, TransformConfiguration
from data_processing.utils import UnrecoverableException, CLIArgumentProvider
from fdedup.transforms.base import threshold_key, num_permutations_key

# configuration parameters
short_name = "fdedup_bucket_processor"
cli_prefix = f"{short_name}_"
minhash_snapshot_directory_key = "minhash_snapshot_directory"
bucket_processor_num_permutations_cli_param = f"{cli_prefix}{num_permutations_key}"
bucket_processor_threshold_cli_param = f"{cli_prefix}{threshold_key}"
bucket_processor_minhash_snapshot_directory_cli_param = f"--{cli_prefix}{minhash_snapshot_directory_key}"

# Execution parameter
LONG_BUCKET = 5000


class FdedupBucketProcessorTransformBase(AbstractBinaryTransform):
    """
    Implements fuzzy dedup Bucket Processor (removing duplicates).
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters
        """
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        super().__init__(config)
        self.buckets = None

    def transform_binary(self, file_name: str, byte_array: bytes) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :param byte_array: contents of the input file to be transformed.
        :param file_name: the name of the file containing the given byte_array.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        self.logger.debug(f"Processing file {file_name}")
        try:
            self.buckets = pickle.loads(byte_array)
        except Exception as e:
            self.logger.warning(f"Failed to load buckets collector with exception {e}")
            raise UnrecoverableException("Failed to unpickle buckets")
        # split buckets into short and long. Long buckets can take very long to process
        long_buckets = []
        short_buckets = []
        while len(self.buckets) > 0:
            b_hash, bucket = self.buckets.popitem()
            if len(bucket) > LONG_BUCKET:
                # It is long
                if len(bucket) > 2 * LONG_BUCKET:
                    # For very long buckets, split them
                    self.logger.info(f"Splitting bucket of length {len(bucket)} into chunks")
                    smaller_buckets = [
                        bucket[i * LONG_BUCKET: (i + 1) * LONG_BUCKET]
                        for i in range((len(bucket) + LONG_BUCKET - 1) // LONG_BUCKET)
                    ]
                    long_buckets.append((b_hash, smaller_buckets))
                else:
                    long_buckets.append((b_hash, bucket))
            else:
                short_buckets.append((b_hash, bucket))
        self.logger.debug(f"processing buckets {len(long_buckets)} long, {len(short_buckets)} short")

        # process long buckets first - we are submitting them one at a time
        for bucket in long_buckets:
            self._submit_bucket_processing([bucket])
        self.logger.debug("Done submitting long buckets")

        # And now the rest of buckets
        bucket_chunks = [short_buckets[i * 100: (i + 1) * 100] for i in range((len(short_buckets) + 99) // 100)]
        for b in bucket_chunks:
            self._submit_bucket_processing(b)
        return [], {"long buckets": len(long_buckets), "short buckets": len(short_buckets)}

    def _submit_bucket_processing(self, buckets: list[tuple[int, list[int]]]) -> None:
        """
        Submit buckets for processing. We are doing this to achieve better processing parallelism
        :param buckets: buckets
        :return: None
        """
        raise NotImplementedError


class FdedupBucketProcessorTransformConfigurationBase(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self, transform_class: type[AbstractBinaryTransform]):
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
            f"--{bucket_processor_num_permutations_cli_param}",
            type=int,
            default=64,
            help="number of permutations")
        parser.add_argument(
            f"--{bucket_processor_threshold_cli_param}",
            type=float,
            default=0.8,
            help="threshold"
        )
        # by default, snapshot file is from the output directory. This parameter can overwrite
        # default location of minhash/buckets snapshot by explicitly defining the snapshot directory
        parser.add_argument(
            f"--{bucket_processor_minhash_snapshot_directory_cli_param}",
            type=str,
            default=None,
            help="location of minhash snapshot files"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True
