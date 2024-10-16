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

import csv
import io
import uuid
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import GB, CLIArgumentProvider, TransformUtils, UnrecoverableException


short_name = "profiler"
cli_prefix = f"{short_name}_"
doc_column_name_key = "doc_column"
doc_column_name_cli_param = f"{cli_prefix}{doc_column_name_key}"


class DataAggregator:
    """
    Implements an element of distributed cache of data
    """

    def __init__(self, params: dict[str, Any]):
        """
        initialize set of local aggregators
        :param params - dictionary of input parameters.
            data_access - data access factory
        """
        self.words = {}
        self.id = str(uuid.uuid4())
        data_access_factory = params.get("data_access_factory", None)
        if data_access_factory is None:
            raise UnrecoverableException("Data access factory is not defined for the data aggregator")
        self.data_access = data_access_factory.create_data_access()

    def add_words(self, words: dict[str, int]) -> None:
        """
        Add words to cache
        :param words: new words dictionary
        :return: None
        """
        # merge dictionaries updating values
        for k, v in words.items():
            self.words[k] = self.words.get(k, 0) + v

    def get_size(self) -> tuple[int, float]:
        """
        Get size of created aggregators for statistics
        :return: size of the local set and its memory footprint
        """
        return len(self.words), TransformUtils.deep_get_size(self.words) / GB

    def save_data(self) -> tuple[dict[str, Any], int]:
        """
        Save data
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        if len(self.words) == 0:
            # No data
            return {}, 0
        output_folder = self.data_access.get_output_folder()
        if not output_folder.endswith("/"):
            output_folder += "/"
        # convert dictionary to CSV
        s = io.StringIO()
        dict_writer = csv.DictWriter(s, ["word", "count"], extrasaction="ignore")
        for i, datum in enumerate(self.words.items()):
            row = {"word": datum[0], "count": datum[1]}
            dict_writer.writerow(row)
        # reset cursor to the beginning of the StringIO stream
        s.seek(0)
        output_path = f"{output_folder}{self.id}.csv"
        return self.data_access.save_file(path=output_path, data=s.read().encode("utf-8"))


class ProfilerTransformBase(AbstractTableTransform):
    """
    Implements Aggregator table transformer.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of the doc column
            aggregators - list of aggregator actors, references
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of AggregateTableTransformConfiguration class
        super().__init__(config)
        self.doc_column = config.get(doc_column_name_key, "contents")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Building aggregations.
        :param table: table
        :param file_name: name of the file to process
        :return: resulting table, statistics
        """
        from base_tokenizer import tokenize

        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column])
        # Inner variables
        words = {}
        # Compute words count
        for text in table[self.doc_column]:
            # Compute doc hash
            tokens = tokenize(text=str(text))
            for token in tokens:
                words[token] = words.get(token, 0) + 1
        # submit word counts to cache
        self._submit_to_cache(words=words)
        # return
        return [], {}

    def _submit_to_cache(self, words: dict[str, str]) -> None:
        """
        Submits
        :param words: dictionary of word occurrences in document
        :return: None
        """
        raise NotImplementedError


class ProfilerTransformConfigurationBase(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self, transform_class: type[AbstractTableTransform], print_config: bool = True):
        super().__init__(
            name=short_name,
            transform_class=transform_class,
        )
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        self.print_config = print_config

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        parser.add_argument(
            f"--{doc_column_name_cli_param}",
            type=str,
            default="contents",
            help="key for accessing data")

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        if self.print_config:
            self.logger.info(f"profiler params are {self.params}")
        return True
