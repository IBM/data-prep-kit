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
import ray
from data_processing.data_access import (
    DataAccess,
    DataAccessFactory,
    DataAccessFactoryBase,
)
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import TransformUtils, get_logger
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle


logger = get_logger(__name__)

shortname = "proglang_select"
cli_prefix = f"{shortname}_"
lang_allowed_langs_file_key = f"{shortname}_allowed_langs_file"
lang_lang_column_key = f"{shortname}_language_column"
lang_allowed_languages = f"{shortname}_allowed_languages"
lang_data_factory_key = f"{shortname}_data_factory"
lang_output_column_key = f"{shortname}_output_column"
lang_default_output_column = "allowed_language"


def _get_supported_languages(lang_file: str, data_access: DataAccess) -> list[str]:
    logger.info(f"Getting supported languages from file {lang_file}")
    lang_list = data_access.get_file(lang_file).decode("utf-8").splitlines()
    logger.info(f"Supported languages {lang_list}")
    return lang_list


class ProgLangSelectTransform(AbstractTableTransform):
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
        self.output_column = config.get(lang_output_column_key, lang_default_output_column)
        languages_include_ref = config.get(lang_allowed_languages, None)
        if languages_include_ref is None:
            path = config.get(lang_allowed_langs_file_key, None)
            if path is None:
                raise RuntimeError(f"Missing configuration value for key {lang_allowed_langs_file_key}")
            daf = config.get(lang_data_factory_key, None)
            data_access = daf.create_data_access()
            self.languages_include = _get_supported_languages(lang_file=path, data_access=data_access)
        else:
            # This is recommended for production approach. In this case domain list is build by the
            # runtime once, loaded to the object store and can be accessed by actors without additional reads
            try:
                logger.info(f"Loading languages to include from Ray storage under reference {languages_include_ref}")
                self.languages_include = ray.get(languages_include_ref)
            except Exception as e:
                logger.info(f"Exception loading languages list from ray object storage {e}")
                raise RuntimeError(f"exception loading from object storage for key {languages_include_ref}")

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:
        """
        Select the rows for which the column `self.lang_column` has a value in the list `self.languages_include`.
        """
        # Ensure that the column exists
        if not TransformUtils.validate_columns(table=table, required=[self.lang_column]):
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
        out_table = TransformUtils.add_column(table, self.output_column, pa.array(mask_known))
        return [out_table], {
            "documents with supported languages": known_count,
            "documents with unsupported languages": unknown_count,
        }


class ProgLangSelectRuntime(DefaultRayTransformRuntime):
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
        data_access_factory: DataAccessFactoryBase,
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
            raise RuntimeError(f"Missing configuration key {lang_allowed_langs_file_key}")
        lang_data_access_factory = self.params.get(lang_data_factory_key, None)
        if lang_data_access_factory is None:
            raise RuntimeError(f"Missing configuration key {lang_data_factory_key}")
        lang_list = _get_supported_languages(
            lang_file=lang_file,
            data_access=lang_data_access_factory.create_data_access(),
        )
        lang_refs = ray.put(list(lang_list))
        logger.info(f"Placed language list into Ray object storage under reference{lang_refs}")
        return {lang_allowed_languages: lang_refs} | self.params


class ProgLangSelectTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name=shortname,
            transform_class=ProgLangSelectTransform,
            remove_from_metadata=[lang_data_factory_key],
        )
        self.daf = None

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the ProgLangMatchTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{lang_allowed_langs_file_key}",
            type=str,
            required=False,
            default=None,
            help="Path to file containing the list of languages to be matched.",
        )
        parser.add_argument(
            f"--{lang_lang_column_key}",
            type=str,
            required=False,
            default="language_column",
            help="The column name holding the name of the programming language assigned to the document",
        )
        parser.add_argument(
            f"--{lang_output_column_key}",
            type=str,
            required=False,
            default=lang_default_output_column,
            help="The column name to add and that contains the matching information",
        )
        # Create the DataAccessFactor to use CLI args
        self.daf = DataAccessFactory(cli_prefix, False)
        # Add the DataAccessFactory parameters to the transform's configuration parameters.
        self.daf.add_input_params(parser)

    def apply_input_params(self, args: Namespace) -> bool:
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
            lang_data_factory_key: self.daf,
            lang_output_column_key: dargs.get(lang_output_column_key, None),
        }
        # Validate and populate the transform's DataAccessFactory
        return self.daf.apply_input_params(args)


class ProgLangSelectPythonConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=ProgLangSelectTransformConfiguration(), runtime_class=ProgLangSelectRuntime)


class ProgLangSelectRayConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=ProgLangSelectTransformConfiguration(), runtime_class=ProgLangSelectRuntime)


if __name__ == "__main__":
    launcher = RayTransformLauncher(ProgLangSelectRayConfiguration())
    launcher.launch()
