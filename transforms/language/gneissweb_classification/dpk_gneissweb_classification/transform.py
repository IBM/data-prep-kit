# SPDX-License-Identifier: Apache-2.0
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
import ast
import os
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, load_model
from dpk_gneissweb_classification.classification_models import (
    ClassificationModel,
    FastTextModel,
)
from dpk_gneissweb_classification.nlp import get_label_ds_pa
from dpk_gneissweb_classification.nlp_parallel import get_label_ds_pa_parallel


short_name = "gcls"
cli_prefix = f"{short_name}_"
model_credential_key = "model_credential"
model_file_name_key = "model_file_name"
model_url_key = "model_url"
content_column_name_key = "content_column_name"
output_label_column_name_key = "output_label_column_name"
output_score_column_name_key = "output_score_column_name"
n_processes_key = "n_processes"
model_credential_cli_param = f"{cli_prefix}{model_credential_key}"
model_file_name_cli_param = f"{cli_prefix}{model_file_name_key}"
model_url_cli_param = f"{cli_prefix}{model_url_key}"
content_column_name_cli_param = f"{cli_prefix}{content_column_name_key}"
output_label_column_name_cli_param = f"{cli_prefix}{output_label_column_name_key}"
output_score_column_name_cli_param = f"{cli_prefix}{output_score_column_name_key}"
n_processes_cli_param = f"{cli_prefix}{n_processes_key}"

default_content_column_name = "contents"
default_output_label_column_name = ["['lang']"]
default_output_score_column_name = ["['score']"]
default_n_processes = 1


class ClassificationTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    nlp_classfication: ClassificationModel
    content_column_name: str
    output_label_column_name: str
    output_score_column_name: str

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, ClassificationTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of ClassificationTransformConfiguration class
        super().__init__(config)

        self.model_credential = config.get(model_credential_cli_param, os.environ.get("HF_READ_ACCESS_TOKEN", None))
        self.model_file_name = ast.literal_eval(config.get(model_file_name_cli_param)[0])
        self.model_url = ast.literal_eval(config.get(model_url_cli_param)[0])
        self.model = []
        for url, model_filename in zip(self.model_url, self.model_file_name):
            self.logger.info(f"Loading Model: {url=}, {model_filename=} ")
            model = load_model(url, "fasttext", self.model_credential, model_filename=model_filename)
            self.model.append(model)
            self.logger.info(f"Loading Model: {url=}, {model_filename=} complete")

        self.n_processes = config.get(n_processes_cli_param, default_n_processes)
        self.content_column_name = config.get(content_column_name_cli_param, default_content_column_name)
        self.output_label_column_name = ast.literal_eval(
            config.get(output_label_column_name_cli_param, default_output_label_column_name)[0]
        )
        self.output_score_column_name = ast.literal_eval(
            config.get(output_score_column_name_cli_param, default_output_score_column_name)[0]
        )

    def transform(
        self, table: pa.Table, file_name: str | None = None
    ) -> tuple[list[pa.Table], dict[str, Any]]:  # pylint:disable=unused-argument
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """

        for label_column_name, score_column_name in zip(self.output_label_column_name, self.output_score_column_name):
            TransformUtils.validate_columns(table, [self.content_column_name])
            if label_column_name in table.schema.names:
                raise Exception(f"column to store label ({label_column_name}) already exist")
            if score_column_name in table.schema.names:
                raise Exception(f"column to store score of label ({score_column_name}) already exist")
        self.logger.debug(f"Transforming one table with {len(table)} rows")
        all_stats: dict[str, Any] = {}
        for model, url, label_column_name, score_column_name in zip(
            self.model, self.model_url, self.output_label_column_name, self.output_score_column_name
        ):
            table, stats = get_label_ds_pa(
                table, model, url, self.content_column_name, label_column_name, score_column_name, self.n_processes
            )
            all_stats.update(stats)

        self.logger.debug(f"Transformed one table with {len(table)} rows")
        return [table], all_stats


class ClassificationTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=ClassificationTransform,
            remove_from_metadata=[model_credential_cli_param],
        )
        from data_processing.utils import get_dpk_logger

        self.logger = get_dpk_logger()

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the ClassificationTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{model_credential_cli_param}",
            help="Credential to access huggingface model",
        )
        parser.add_argument(
            f"--{model_file_name_cli_param}",
            type=str,
            nargs="+",
            default="",
            help="filename of model",
        )
        parser.add_argument(f"--{model_url_cli_param}", type=str, nargs="+", default="", help="Url to model")
        parser.add_argument(
            f"--{content_column_name_cli_param}",
            default=default_content_column_name,
            help="Column name to get content",
        )
        parser.add_argument(
            f"--{output_label_column_name_cli_param}",
            default=default_output_label_column_name,
            type=str,
            nargs="+",
            help="Column name to store label",
        )
        parser.add_argument(
            f"--{output_score_column_name_cli_param}",
            default=default_output_score_column_name,
            type=str,
            nargs="+",
            help="Column name to store the score",
        )
        parser.add_argument(
            f"--{n_processes_cli_param}",
            type=int,
            default=default_n_processes,
            help="number of processes. Must be a positive integer.",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, True)
        self.params = self.params | captured
        self.logger.info(f"parameters are : {self.params}")
        return True
