import argparse
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import TransformUtils, str2bool, get_logger
from data_processing.data_access import DataAccessFactory, DataAccess

import ray
from ray.actor import ActorHandle


logger = get_logger(__name__)

ls_allowed_langs_file_key = "ls_allowed_langs_file"
ls_lang_column_key = "ls_language_column"
ls_known_selector = "ls_return_known"
ls_allowed_languages = "ls_allowed_languages"


def _get_supported_languages(lang_file: str, data_access: DataAccess) -> list[str]:
    logger.info(f"Getting supported languages from file {lang_file}")
    lang_list = data_access.get_file(lang_file).decode("utf-8").splitlines()
    logger.info(f"Supported languages {lang_list}")
    return lang_list


class LangSelectorTransform(AbstractTableTransform):
    """ """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, LangSelectorTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """

        super().__init__(config)
        languages_include_ref = config.get(ls_allowed_languages, None)
        if languages_include_ref is None:
            raise RuntimeError(f"Missing configuration value for key {ls_allowed_languages}")
        self.languages_include = ray.get(languages_include_ref)
        self.lang_column = config.get(ls_lang_column_key, "")
        self.known = config.get(ls_known_selector, True)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:
        """
        Select the rows for which the column `self.lang_column` has a value in the list `self.languages_include`.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the
        output folder, without modification.)
        """
        # Ensure that the column exists
        if not TransformUtils.validate_columns(table=table, required=[self.lang_column]):
            return [], {}
        # remove languages that we do not support
        mask_known = [False] * table.num_rows
        mask_unknown = [False] * table.num_rows
        index = 0
        known_count = 0
        for lang in table[self.lang_column]:
            if str(lang) in self.languages_include:
                mask_known[index] = True
                known_count += 1
            else:
                mask_unknown[index] = True
            index += 1
        unknown_count = table.num_rows - known_count
        # pick the table to return
        if self.known:
            out_table = table.filter(mask_known)
        else:
            out_table = table.filter(mask_unknown)
        return [out_table], {
            "supported languages": known_count,
            "unsupported languages": unknown_count,
        }


class LangSelectorRuntime(DefaultTableTransformRuntime):
    """
    Language selector runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            ls_lang_column_key: name of the column with language
            ls_allowed_langs_file_key: location of the allowed languages file
            ls_known_selector: A flag on whether return rows with valid or invalid languages
        """
        super().__init__(params)

    def get_transform_config(
            self, data_access_factory: DataAccessFactory, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        # create the list of blocked domains by reading the files at the conf_url location
        lang_file = self.params.get(ls_allowed_langs_file_key, None)
        if lang_file is None:
            raise RuntimeError(f"Missing configuration key {ls_allowed_langs_file_key}")
        data_access = data_access_factory.create_data_access()
        lang_list = _get_supported_languages(lang_file, data_access)
        lang_refs = ray.put(list(lang_list))
        return {ls_allowed_languages: lang_refs} | self.params


class LangSelectorTransformConfiguration(DefaultTableTransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name="lang_select",
            transform_class=LangSelectorTransform,
            runtime_class=LangSelectorRuntime,
        )
        self.params = {}

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the LangSelectorTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{ls_allowed_langs_file_key}",
            type=str,
            required=False,
            default=None,
            help="Directory to store unknown language files.",
        )
        parser.add_argument(
            f"--{ls_lang_column_key}",
            type=str,
            required=False,
            default="language_column",
            help="Directory to store unknown language files.",
        )
        parser.add_argument(
            f"--{ls_known_selector}",
            type=lambda x: bool(str2bool(x)),
            required=False,
            default=True,
            help="Flag to return docs with known languages (True) or unknown {False}.",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        dargs = vars(args)
        if dargs.get(ls_allowed_langs_file_key, None) is None:
            logger.info(f"{ls_allowed_langs_file_key} is required, but got None")
            return False
        if dargs.get(ls_lang_column_key, None) is None:
            logger.info(f"{ls_lang_column_key} is required, but got None")
            return False
        self.params = {ls_lang_column_key: dargs.get(ls_lang_column_key, None),
                       ls_allowed_langs_file_key: dargs.get(ls_allowed_langs_file_key, None),
                       ls_known_selector: dargs.get(ls_known_selector, True)}
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=LangSelectorTransformConfiguration())
    launcher.launch()
