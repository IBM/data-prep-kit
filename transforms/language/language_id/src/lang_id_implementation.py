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
from watson_nlp import get_lang_ds_pa


"""
Reference https://github.ibm.com/ai-foundation/foundation-model-stack/tree/main/preprocessing/ray/language_identification_sentence_split
"""


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

        self.nlp_langid = LangModelFactory.create_model(config["model_kind"], config.get("model_path", None))

        if "drop_column_if_existed" in config:
            self.drop_column_if_existed = config["drop_column_if_existed"]
        else:
            self.drop_column_if_existed = True

    def transform(self, table: pa.Table) -> list[pa.Table]:

        if not TransformUtils.validate_columns(table, ["contents"]):
            exit(1)

        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.
        """
        new_columns = ["ft_lang", "ft_score"]
        if not self.drop_column_if_existed and TransformUtils.validate_columns(table, new_columns):
            exit(1)

        table, stats = get_lang_ds_pa(table, self.nlp_langid)

        # TODO: submit stats to the statistics service passed in from configuration
        return [table], {}


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
        parser.add_argument("-dr", "--drop_column_if_existed", default=True, help="drop columns if existed")
        parser.add_argument("--model_kind", default="", help="Kind of model for language detection")
        parser.add_argument("--model_path", default=None, help="Path to model for language detection")

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments including at least, but perhaps more,
        arguments as defined by add_input_arguments().
        :return: True, if validate pass or False otherwise
        """
        self.params["drop_column_if_existed"] = args.drop_column_if_existed
        self.params["model_kind"] = args.model_kind
        self.params["model_path"] = args.model_path
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return:
        """
        return self.params
