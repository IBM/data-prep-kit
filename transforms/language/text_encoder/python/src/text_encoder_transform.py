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

import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils
from sentence_transformers import SentenceTransformer


short_name = "text_encoder"
cli_prefix = f"{short_name}_"
model_name_key = "model_name"
content_column_name_key = "content_column_name"
output_embeddings_column_name_key = "output_embeddings_column_name"
model_name_cli_param = f"{cli_prefix}{model_name_key}"
content_column_name_cli_param = f"{cli_prefix}{content_column_name_key}"
output_embeddings_column_name_cli_param = f"{cli_prefix}{output_embeddings_column_name_key}"

default_model_name = "BAAI/bge-small-en-v1.5"
default_content_column_name = "contents"
default_output_embeddings_column_name = "embeddings"


class TextEncoderTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """ """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of TextEncoderTransform class
        super().__init__(config)
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

        self.model_name = config.get(model_name_key, default_model_name)
        self.content_column_name = config.get(content_column_name_key, default_content_column_name)
        self.output_embeddings_column_name = config.get(
            output_embeddings_column_name_key, default_output_embeddings_column_name
        )

        self.model = SentenceTransformer(self.model_name)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """ """
        self.logger.debug(f"Transforming one table with {len(table)} rows")

        # make sure that the content column exists
        TransformUtils.validate_columns(table=table, required=[self.content_column_name])

        embeddings = list(
            map(
                lambda x: self.model.encode(x, normalize_embeddings=True),
                table[self.content_column_name].to_pylist(),
            ),
        )
        result = TransformUtils.add_column(table=table, name=self.output_embeddings_column_name, content=embeddings)

        metadata = {"nfiles": 1, "nrows": len(result)}
        return [result], metadata


class TextEncoderTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=TextEncoderTransform,
            # remove_from_metadata=[pwd_key],
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__ + "cfg")  # workaround issue #481

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the TextEncoderTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{content_column_name_cli_param}",
            default=default_content_column_name,
            help="Name of the column containing the text to be encoded",
        )
        parser.add_argument(
            f"--{output_embeddings_column_name_cli_param}",
            default=default_output_embeddings_column_name,
            help="Column name to store the embeddings",
        )
        parser.add_argument(
            f"--{model_name_cli_param}",
            default=default_model_name,
            help=f"Name of the HF model to use for encoding the text. The default model is {default_model_name}",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)

        self.params = self.params | captured
        self.logger.info(f"text_encoder parameters are : {self.params}")
        return True
