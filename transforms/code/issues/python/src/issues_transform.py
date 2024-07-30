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

import json
import logging
import uuid
from argparse import ArgumentParser, Namespace
from typing import Any
from functools import partial

import pyarrow as pa
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.transform import (
    AbstractTableTransform,
    TransformConfiguration,
)
from data_processing.utils import CLIArgumentProvider, TransformUtils, get_logger
from utils_issues import (
    filter_on_users_size,
    merge_text_columns,
    remove_bot_comments,
    replace_usernames,
    strip_automated_email_text,
    truncate_long_comments,
)

logger = get_logger(__name__)

short_name = "issues"
cli_prefix = f"{short_name}_"

min_chars_key = "min_chars"
max_chars_key = "max_chars"
max_events_key = "max_events"
max_lines_key = "max_lines"
events_column_name_key = "events_column_name"
min_chars_cli_param = f"{cli_prefix}{min_chars_key}"
max_chars_cli_param = f"{cli_prefix}{max_chars_key}"
max_events_cli_param = f"{cli_prefix}{max_events_key}"
max_lines_cli_param = f"{cli_prefix}{max_lines_key}"
events_column_name_cli_param = f"{cli_prefix}{events_column_name_key}"

default_min_chars = 200
default_max_chars = 7000
default_max_events = 10
default_max_lines = 80
default_events_column_name = "events"

STATNAME_SIZE_BEFORE_FILTER = 'size_before_filter'
STATNAME_SIZE_GB_BEFORE_FILTER = 'size_gb_before_filter'
STATNAME_SIZE_AFTER_LONG_COMMENTS_TRUNCATE = 'size_after_long_comments_truncate'
STATNAME_SIZE_GB_AFTER_LONG_COMMENTS_TRUNCATE = 'size_gb_after_long_comments_truncate'
STATNAME_PERCENTAGE_SIZE_AFTER_LONG_COMMENTS_TRUNCATE = 'percentage_size_after_long_comments_truncate'
STATNAME_PERCENTAGE_SIZE_GB_AFTER_LONG_COMMENTS_TRUNCATE = 'percentage_size_gb_after_long_comments_truncate'
STATNAME_SIZE_AFTER_BOT_FILTER = 'size_after_bot_filter'
STATNAME_SIZE_GB_AFTER_BOT_FILTER = 'size_gb_after_bot_filter'
STATNAME_PERCENTAGE_SIZE_AFTER_BOT_FILTER = 'percentage_size_after_bot_filter'
STATNAME_PERCENTAGE_SIZE_GB_AFTER_BOT_FILTER = 'percentage_size_gb_after_bot_filter'
STATNAME_SIZE_AFTER_USERS_FILTER = 'size_after_users_filter'
STATNAME_SIZE_GB_AFTER_USERS_FILTER = 'size_gb_after_users_filter'
STATNAME_PERCENTAGE_SIZE_AFTER_USERS_FILTER = 'percentage_size_after_users_filter'
STATNAME_PERCENTAGE_SIZE_GB_AFTER_USERS_FILTER = 'percentage_size_gb_after_users_filter'

class IssuesTransform(AbstractTableTransform):
    """
    Implements a transform to filter issues.
    """
    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, RepoLevelOrderTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of IssuesTransformConfiguration class
        super().__init__(config)
        self.min_chars = config.get(min_chars_key, default_min_chars)
        self.max_chars = config.get(max_chars_key, default_max_chars)
        self.max_events = config.get(max_events_key, default_max_events)
        self.max_lines = config.get(max_lines_key, default_max_lines)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        metadata = {
            STATNAME_SIZE_BEFORE_FILTER: 0,
            STATNAME_SIZE_GB_BEFORE_FILTER: 0,
            STATNAME_SIZE_AFTER_LONG_COMMENTS_TRUNCATE: 0,
            STATNAME_SIZE_GB_AFTER_LONG_COMMENTS_TRUNCATE: 0,
            STATNAME_SIZE_AFTER_BOT_FILTER: 0,
            STATNAME_SIZE_GB_AFTER_BOT_FILTER: 0,
            STATNAME_SIZE_AFTER_USERS_FILTER: 0,
            STATNAME_SIZE_GB_AFTER_USERS_FILTER: 0,
        }
        filtered = []
        for batch in table.to_batches():
            examples = batch.to_pydict()
            for events in examples['events']:
                example = {'events': events}
                # basic processing
                example = merge_text_columns(example)
                example = strip_automated_email_text(example)
                size, size_gb = self._collect_stats(example)
                metadata[STATNAME_SIZE_BEFORE_FILTER] = metadata[STATNAME_SIZE_BEFORE_FILTER] + size
                metadata[STATNAME_SIZE_GB_BEFORE_FILTER] = metadata[STATNAME_SIZE_GB_BEFORE_FILTER] + size_gb
                # truncate long comments
                example = partial(truncate_long_comments, max_lines=self.max_lines)(example)
                size, size_gb = self._collect_stats(example)
                metadata[STATNAME_SIZE_AFTER_LONG_COMMENTS_TRUNCATE] = metadata[STATNAME_SIZE_AFTER_LONG_COMMENTS_TRUNCATE] + size
                metadata[STATNAME_SIZE_GB_AFTER_LONG_COMMENTS_TRUNCATE] = metadata[STATNAME_SIZE_GB_AFTER_LONG_COMMENTS_TRUNCATE] + size_gb
                # bot filter
                example = remove_bot_comments(example)
                if example['bot_issue']:
                    continue
                size, size_gb = self._collect_stats(example)
                metadata[STATNAME_SIZE_AFTER_BOT_FILTER] = metadata[STATNAME_SIZE_AFTER_BOT_FILTER] + size
                metadata[STATNAME_SIZE_GB_AFTER_BOT_FILTER] = metadata[STATNAME_SIZE_GB_AFTER_BOT_FILTER] + size_gb
                example['user_count'] = len(set(event["author"] for event in example["events"]))
                example['event_count'] = len(example['events'])
                example['text_size']  = size_gb
                if not partial(filter_on_users_size, minimum=self.min_chars, maximum=self.max_chars, max_events=self.max_events)(example):
                    continue
                size, size_gb = self._collect_stats(example)
                metadata[STATNAME_SIZE_AFTER_BOT_FILTER] = metadata[STATNAME_SIZE_AFTER_BOT_FILTER] + size
                metadata[STATNAME_SIZE_GB_AFTER_BOT_FILTER] = metadata[STATNAME_SIZE_GB_AFTER_BOT_FILTER] + size_gb
                # replace usernames
                example = replace_usernames(example)

                filtered.append(example)
        print(metadata)
        return [pa.Table.from_struct_array(pa.array(filtered))], metadata
    
    def _collect_stats(self, example: dict) -> tuple[int, int]:
        return 1, sum([len(event["text"]) for event in example["events"]])

class IssuesTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=IssuesTransform,
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the DocQualityTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """


        """We filter out short files and those with only one user, except if the size
        of text in comments is between minimum and maximum characters
        and issue has less than max_events events.
        """
        parser.add_argument(
            f"--{min_chars_cli_param}",
            type=int,
            default=default_min_chars,
            help="The minimum characters that text of issue with only one user is required to contain",
        )
        parser.add_argument(
            f"--{max_chars_cli_param}",
            type=int,
            default=default_max_chars,
            help="The maximum characters that text of issue with only one user is allowed to contain",
        )
        parser.add_argument(
            f"--{max_events_cli_param}",
            type=int,
            default=default_max_events,
            help="The maximum events that issue with only one user is allowed to contain",
        )
        parser.add_argument(
            f"--{max_lines_cli_param}",
            type=int,
            default=default_max_lines,
            help="The maximum lines that text of issue can hold. The longer text will be truncated.",
        )
        parser.add_argument(
            f"--{events_column_name_cli_param}",
            required=True,
            default=default_events_column_name,
            help="The column name to get the list of events.",
        )
        
    
    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        logger.info(f"issues parameters are : {self.params}")
        return True