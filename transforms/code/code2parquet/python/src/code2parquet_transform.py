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

import io
import json
import logging
import uuid
import zipfile
from argparse import ArgumentParser, Namespace
from datetime import datetime
from typing import Any

import pyarrow as pa
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.transform import (
    AbstractBinaryTransform,
    AbstractTransform,
    TransformConfiguration,
)
from data_processing.utils import CLIArgumentProvider, TransformUtils, str2bool


shortname = "code2parquet"
cli_prefix = f"{shortname}_"

supported_langs_file_key = "supported_langs_file"
supported_langs_file_cli_key = f"{cli_prefix}{supported_langs_file_key}"

supported_languages_key = "supported_languages"
supported_languages_cli_key = f"{cli_prefix}{supported_languages_key}"

detect_programming_lang_key = "detect_programming_lang"
detect_programming_lang_cli_key = f"{cli_prefix}{detect_programming_lang_key}"
detect_programming_lang_default = True

data_factory_key = "data_factory"

domain_key = "domain"
domain_cli_key = f"{cli_prefix}{domain_key}"
snapshot_key = "snapshot"
snapshot_cli_key = f"{cli_prefix}{snapshot_key}"


def get_supported_languages(lang_file: str, data_access: DataAccess, logger: logging.Logger) -> dict[str, str]:
    logger.debug(f"Getting supported languages from file {lang_file}")
    json_data, _ = data_access.get_file(lang_file)
    lang_dict = json.loads(json_data.decode("utf-8"))
    reversed_dict = {ext: langs for langs, exts in lang_dict.items() for ext in exts}
    logger.debug(f"Supported languages {reversed_dict}")
    return reversed_dict


class CodeToParquetTransform(AbstractBinaryTransform):
    def __init__(self, config: dict):
        """

        Args:
            config: dictionary of configuration data
                supported_langs - dictionary of file extenstions to language names.
                supported_langs_file - if supported_langs, is not provided, then read a map
                    of language names keyed to a list of extensions, from this json file.  The file is read using
                    the DataAccessFactory, under the code2parquet_data_factory key.
        """
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)
        super().__init__(config)
        self.languages_supported = config.get(supported_languages_key, None)
        if self.languages_supported is None:
            path = config.get(supported_langs_file_key, None)
            if path is not None:
                daf = config.get(data_factory_key, None)
                if daf is None:
                    raise ValueError(f"Neither {supported_languages_key} nor {data_factory_key} were provided.")
                data_access = daf.create_data_access()
                self.languages_supported = get_supported_languages(
                    lang_file=path, data_access=data_access, logger=self.logger
                )
            self.detect_programming_lang = config.get(detect_programming_lang_key, detect_programming_lang_default)
            if self.detect_programming_lang and self.languages_supported is None:
                raise RuntimeError(
                    "Programming language detection requested without providing a mapping of extensions to languages"
                )
        domain = config.get(domain_key, None)
        snapshot = config.get(domain_key, None)
        self.shared_columns = {}
        if domain is not None:
            self.shared_columns["domain"] = domain
        if snapshot is not None:
            self.shared_columns["snapshot"] = snapshot

    def _get_lang_from_ext(self, ext):
        lang = "unknown"
        if ext is not None:
            lang = self.languages_supported.get(ext, lang)
        return lang

    def transform_binary(self, file_name: str, byte_array: bytes) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts raw data file (ZIP) to Parquet format
        """
        # We currently only process .zip files
        if TransformUtils.get_file_extension(file_name)[1] != ".zip":
            self.logger.warning(f"Got unsupported file type {file_name}, skipping")
            return [], {}
        data = []
        number_of_rows = 0
        with zipfile.ZipFile(io.BytesIO(bytes(byte_array))) as opened_zip:
            # Loop through each file member in the ZIP archive
            for member in opened_zip.infolist():
                if not member.is_dir():
                    with opened_zip.open(member) as file:
                        try:
                            # Read the content of the file
                            content_bytes = file.read()
                            # Decode the content
                            content_string = TransformUtils.decode_content(content_bytes)
                            if content_string and len(content_string) > 0:
                                ext = TransformUtils.get_file_extension(member.filename)[1]
                                row_data = {
                                    "title": member.filename,
                                    "document": TransformUtils.get_file_basename(file_name),
                                    "contents": content_string,
                                    "document_id": str(uuid.uuid4()),
                                    "ext": ext,
                                    "hash": TransformUtils.str_to_hash(content_string),
                                    "size": len(content_string),
                                    "date_acquired": datetime.now().isoformat(),
                                } | self.shared_columns
                                if self.detect_programming_lang:
                                    lang = self._get_lang_from_ext(ext)
                                    row_data["programming_language"] = lang  # TODO column name should be configurable
                                data.append(row_data)
                                number_of_rows += 1
                            else:
                                self.logger.warning(
                                    f"file {member.filename} is empty. content {content_string}, skipping"
                                )
                        except Exception as e:
                            self.logger.warning(f"Exception {str(e)} processing file {member.filename}, skipping")
        table = pa.Table.from_pylist(data)
        return [(TransformUtils.convert_arrow_to_binary(table=table), ".parquet")], {"number of rows": number_of_rows}


class CodeToParquetTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self, transform_class: type[AbstractTransform] = CodeToParquetTransform):
        super().__init__(
            name=shortname,
            transform_class=transform_class,
            remove_from_metadata=[data_factory_key],
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)
        self.daf = None

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the ProgLangMatchTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{cli_prefix}{supported_langs_file_key}",
            type=str,
            default=None,
            help="Path to file containing the list of supported languages",
        )
        parser.add_argument(
            f"--{cli_prefix}{detect_programming_lang_key}",
            type=lambda x: bool(str2bool(x)),
            default=detect_programming_lang_default,
            help="Infer the programming lang from the file extension using the file of supported languages",
        )
        parser.add_argument(
            f"--{snapshot_cli_key}", type=str, help="Snapshot value assigned to all imported documents.", default=None
        )
        parser.add_argument(
            f"--{domain_cli_key}",
            type=str,
            help="Domain value assigned to all imported documents.",
            default=None,
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
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(supported_langs_file_key, None) is None:
            self.logger.warning(f"{supported_langs_file_key} is required, but got None")
            return False
        self.params = captured | {
            # detect_programming_lang_key: captured.get(detect_programming_lang_key, None),
            # supported_langs_file_key: captured.get(supported_langs_file_key, ""),
            #            domain_key: dargs.get(domain_key, ""),
            # snapshot_key: dargs.get(snapshot_key, ""),
            data_factory_key: self.daf,
        }
        # self.logger.info(f"Transform configuration {self.params}") # Uhmm, let's NOT print out S3 keys please!

        # Validate and populate the transform's DataAccessFactory
        return self.daf.apply_input_params(args)
