import argparse
from typing import Any

import pyarrow as pa
import ray
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import TransformUtils, get_logger, str2bool
from ray.actor import ActorHandle


logger = get_logger(__name__)

lang_allowed_langs_file_key = "lang_select_allowed_langs_file"
lang_lang_column_key = "lang_select_language_column"
lang_known_selector = "lang_select_return_known"
lang_allowed_languages = "lang_select_allowed_languages"
lang_data_factory_key = "lang_select_data_factory"
lang_output_column_key = "lang_select_output_column"
lang_default_output_column = "allowed_language"


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
        self.lang_column = config.get(lang_lang_column_key, "")
        self.output_column = config.get(
            lang_output_column_key, lang_default_output_column
        )
        self.known = config.get(lang_known_selector, True)
        languages_include_ref = config.get(lang_allowed_languages, None)
        if languages_include_ref is None:
            path = config.get(lang_allowed_langs_file_key, None)
            if path is None:
                raise RuntimeError(
                    f"Missing configuration value for key {lang_allowed_langs_file_key}"
                )
            daf = config.get(lang_data_factory_key, None)
            if daf is None:
                raise RuntimeError(
                    f"Missing configuration value for key {lang_data_factory_key}"
                )
            self.languages_include = _get_supported_languages(
                lang_file=path, data_access=daf.create_data_access()
            )
        else:
            # This is recommended for production approach. In this case domain list is build by the
            # runtime once, loaded to the object store and can be accessed by actors without additional reads
            try:
                logger.info(
                    f"Loading languages to include from Ray storage under reference {languages_include_ref}"
                )
                self.languages_include = ray.get(languages_include_ref)
            except Exception as e:
                logger.info(
                    f"Exception loading languages list from ray object storage {e}"
                )
                raise RuntimeError(
                    f"exception loading from object storage for key {languages_include_ref}"
                )

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:
        """
        Select the rows for which the column `self.lang_column` has a value in the list `self.languages_include`.
        """
        # Ensure that the column exists
        if not TransformUtils.validate_columns(
            table=table, required=[self.lang_column]
        ):
            return [], {}

        mask_known = [False] * table.num_rows

        index = 0
        known_count = 0
        for lang in table[self.lang_column]:
            if str(lang) in self.languages_include:
                mask_known[index] = True
                known_count += 1
            index += 1
        unknown_count = table.num_rows - known_count
        # pick the table to return
        out_table = TransformUtils.add_column(
            table, self.output_column, pa.array(mask_known)
        )
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
        self,
        data_access_factory: DataAccessFactory,
        statistics: ActorHandle,
        files: list[str],
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        lang_file = self.params.get(lang_allowed_langs_file_key, None)
        if lang_file is None:
            raise RuntimeError(
                f"Missing configuration key {lang_allowed_langs_file_key}"
            )
        lang_data_access_factory = self.params.get(lang_data_factory_key, None)
        if lang_data_access_factory is None:
            raise RuntimeError(f"Missing configuration key {lang_data_factory_key}")
        lang_list = _get_supported_languages(
            lang_file=lang_file,
            data_access=lang_data_access_factory.create_data_access(),
        )
        lang_refs = ray.put(list(lang_list))
        logger.info(
            f"Placed language list into Ray object storage under reference{lang_refs}"
        )
        return {lang_allowed_languages: lang_refs} | self.params


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
        self.daf = None

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the LangSelectorTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{lang_allowed_langs_file_key}",
            type=str,
            required=False,
            default=None,
            help="Directory to store unknown language files.",
        )
        parser.add_argument(
            f"--{lang_lang_column_key}",
            type=str,
            required=False,
            default="language_column",
            help="Directory to store unknown language files.",
        )
        parser.add_argument(
            f"--{lang_output_column_key}",
            type=str,
            required=False,
            default=lang_default_output_column,
            help="Additional column to be added to output.",
        )
        parser.add_argument(
            f"--{lang_known_selector}",
            type=lambda x: bool(str2bool(x)),
            required=False,
            default=True,
            help="Flag to return docs with known languages (True) or unknown {False}.",
        )
        # Create the DataAccessFactor to use CLI args
        self.daf = DataAccessFactory(f"{self.name}_")
        # Add the DataAccessFactory parameters to the transform's configuration parameters.
        self.daf.add_input_params(parser)

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        dargs = vars(args)
        if dargs.get(lang_allowed_langs_file_key, None) is None:
            logger.info(f"{lang_allowed_langs_file_key} is required, but got None")
            return False
        if dargs.get(lang_lang_column_key, None) is None:
            logger.info(f"{lang_lang_column_key} is required, but got None")
            return False
        self.params = {
            lang_lang_column_key: dargs.get(lang_lang_column_key, None),
            lang_allowed_langs_file_key: dargs.get(lang_allowed_langs_file_key, None),
            lang_known_selector: dargs.get(lang_known_selector, True),
            lang_data_factory_key: self.daf,
            lang_output_column_key: dargs.get(lang_output_column_key, None),
        }
        # remove data access factory from metadata
        self.remove_from_metadata.append(lang_data_factory_key)
        # Validate and populate the transform's DataAccessFactory
        return self.daf.apply_input_params(args)


if __name__ == "__main__":
    launcher = TransformLauncher(
        transform_runtime_config=LangSelectorTransformConfiguration()
    )
    launcher.launch()
