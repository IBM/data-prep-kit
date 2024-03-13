import argparse
from argparse import ArgumentParser
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import TransformUtils
from nlp import get_lang_ds_pa


"""
Reference https://github.ibm.com/ai-foundation/foundation-model-stack/tree/main/preprocessing/ray/language_identification_sentence_split
"""

PARAM_DROP_COLUMN_IF_EXISTED = "drop_column_if_existed"
PARAM_MODEL_CREDENTIAL = "model_credential"
PARAM_MODEL_KIND = "model_kind"
PARAM_MODEL_URL = "model_url"
PARAM_CONTENT_COLUMN_NAME = "content_column_name"


class LangIdentificationTransform(AbstractTableTransform):
    """
    Implements language identification in a pyarrow Table.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments.
        """

        # super().__init__(config)
        from lang_models import LangModelFactory

        self.nlp_langid = LangModelFactory.create_model(
            config[PARAM_MODEL_KIND], config.get(PARAM_MODEL_URL), config.get(PARAM_MODEL_CREDENTIAL)
        )
        self.column_name = config.get(PARAM_CONTENT_COLUMN_NAME)
        if PARAM_DROP_COLUMN_IF_EXISTED in config:
            self.drop_column_if_existed = config[PARAM_DROP_COLUMN_IF_EXISTED]
        else:
            self.drop_column_if_existed = True

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        if not TransformUtils.validate_columns(table, [self.column_name]):
            exit(1)

        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.
        """
        new_columns = ["ft_lang", "ft_score"]
        if not self.drop_column_if_existed and TransformUtils.validate_columns(table, new_columns):
            exit(1)

        table, stats = get_lang_ds_pa(table, self.nlp_langid, self.column_name)
        return [table], stats


class LangIdentificationTableTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name="LangIdentification",
            runtime_class=DefaultTableTransformRuntime,
            transform_class=LangIdentificationTransform,
        )
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument("-dr", f"--{PARAM_DROP_COLUMN_IF_EXISTED}", default=True, help="drop columns if existed")
        parser.add_argument(
            f"--{PARAM_MODEL_CREDENTIAL}",
            default=None,
            help="Credential to access model for language detection placed in url",
        )
        parser.add_argument(f"--{PARAM_MODEL_KIND}", default="", help="Kind of model for language detection")
        parser.add_argument(f"--{PARAM_MODEL_URL}", default=None, help="Url to model for language detection")
        parser.add_argument(f"--{PARAM_CONTENT_COLUMN_NAME}", default="contents", help="Column name to get content")

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments including at least, but perhaps more,
        arguments as defined by add_input_arguments().
        :return: True, if validate pass or False otherwise
        """
        self.params[PARAM_DROP_COLUMN_IF_EXISTED] = args.drop_column_if_existed
        self.params[PARAM_MODEL_CREDENTIAL] = args.model_credential
        self.params[PARAM_MODEL_KIND] = args.model_kind
        self.params[PARAM_MODEL_URL] = args.model_url
        self.params[PARAM_CONTENT_COLUMN_NAME] = args.content_column_name
        self.remove_from_metadata.append(PARAM_MODEL_CREDENTIAL)
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return:
        """
        return self.params
