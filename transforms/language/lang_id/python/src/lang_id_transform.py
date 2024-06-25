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

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils
from lang_models import LangModelFactory
from nlp import get_lang_ds_pa


short_name = "lang_id"
cli_prefix = f"{short_name}_"
model_credential_key = "model_credential"
model_kind_key = "model_kind"
model_url_key = "model_url"
content_column_name_key = "content_column_name"
output_lang_column_name_key = "output_lang_column_name"
output_score_column_name_key = "output_score_column_name"
model_credential_cli_param = f"{cli_prefix}{model_credential_key}"
model_kind_cli_param = f"{cli_prefix}{model_kind_key}"
model_url_cli_param = f"{cli_prefix}{model_url_key}"
content_column_name_cli_param = f"{cli_prefix}{content_column_name_key}"
output_lang_column_name_cli_param = f"{cli_prefix}{output_lang_column_name_key}"
output_score_column_name_cli_param = f"{cli_prefix}{output_score_column_name_key}"

default_content_column_name = "contents"
default_output_lang_column_name = "lang"
default_output_score_column_name = "score"

class LangIdentificationTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, LangIdentificationTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of LangIdentificationTransformConfiguration class
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)
        super().__init__(config)
        self.nlp_langid = LangModelFactory.create_model(
            config.get(model_kind_key), config.get(model_url_key), config.get(model_credential_key)
        )
        self.content_column_name = config.get(content_column_name_key, default_content_column_name)
        self.output_lang_column_name = config.get(output_lang_column_name_key, default_output_lang_column_name)
        self.output_score_column_name = config.get(output_score_column_name_key, default_output_score_column_name)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        TransformUtils.validate_columns(table, [self.content_column_name])
        if self.output_lang_column_name in table.schema.names:
            raise Exception(
                f"column to store identified language ({self.output_lang_column_name}) already exist"
            )
        if self.output_score_column_name in table.schema.names:
            raise Exception(
                f"column to store score of language identification ({self.output_score_column_name}) already exist"
            )
        self.logger.debug(f"Transforming one table with {len(table)} rows")
        table, stats = get_lang_ds_pa(table, self.nlp_langid, self.content_column_name)
        self.logger.debug(f"Transformed one table with {len(table)} rows")
        return [table], stats


class LangIdentificationTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=LangIdentificationTransform,
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the LangIdentificationTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{model_credential_cli_param}",
            required=True,
            help="Credential to access model for language detection placed in url",
        )
        parser.add_argument(f"--{model_kind_cli_param}", required=True, help="Kind of model for language detection")
        parser.add_argument(f"--{model_url_cli_param}", required=True, help="Url to model for language detection")
        parser.add_argument(
            f"--{content_column_name_cli_param}", default=default_content_column_name, help="Column name to get content"
        )
        parser.add_argument(
            f"--{output_lang_column_name_cli_param}", default=default_output_lang_column_name, help="Column name to store identified language"
        )
        parser.add_argument(
            f"--{output_score_column_name_cli_param}", default=default_output_score_column_name, help="Column name to store the score of language identification"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"lang_id parameters are : {self.params}")
        return True
