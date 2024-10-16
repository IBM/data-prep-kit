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

import enum
import io
import time
import zipfile
from argparse import ArgumentParser, Namespace
from datetime import datetime
from typing import Any

import pyarrow as pa
import trafilatura
from data_processing.transform import AbstractBinaryTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, get_logger


# disabled for now
# from data_processing_ray.runtime.ray import RayTransformLauncher
# from data_processing_ray.runtime.ray.runtime_configuration import (
#   RayTransformRuntimeConfiguration,
# )
# import data_processing




class Html2ParquetTransform(AbstractBinaryTransform):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)

        self.output_format = config.get(html2parquet_output_format_key, html2parquet_output_format.MARKDOWN)
        self.favor_precision = config.get(html2parquet_favor_precision_key, html2parquet_favor_precision.TRUE)
        self.favor_recall = config.get(html2parquet_favor_recall_key, html2parquet_favor_recall.TRUE)

        if not isinstance(self.output_format, html2parquet_output_format):
            self.output_format = html2parquet_output_format[self.output_format]

        if not isinstance(self.favor_precision, html2parquet_favor_precision):
            self.favor_precision = html2parquet_favor_precision[self.favor_precision]

        if not isinstance(self.favor_recall, html2parquet_favor_recall):
            self.favor_recall = html2parquet_favor_recall[self.favor_recall]

    def _convert_html2parquet(self, member_filename: str, file_name: str, content_bytes: bytes) -> dict:

        title = member_filename if member_filename else TransformUtils.get_file_basename(file_name)

        output_format_value = str(self.output_format)
        if output_format_value not in ["markdown", "txt"]:
            raise RuntimeError(f"Unknown output_format {self.output_format}.")

        if self.favor_precision == html2parquet_favor_precision.TRUE:
            favor_precision_value = True
        elif self.favor_precision == html2parquet_favor_precision.FALSE:
            favor_precision_value = False
        else:
            raise RuntimeError(f"Unknown favor_precision {self.favor_precision}.")

        if self.favor_recall == html2parquet_favor_recall.TRUE:
            favor_recall_value = True
        elif self.favor_recall == html2parquet_favor_recall.FALSE:
            favor_recall_value = False
        else:
            raise RuntimeError(f"Unknown favor_recall {self.favor_recall}.")

        # Use Trafilatura library
        content_string = trafilatura.extract(
            content_bytes,
            output_format=output_format_value,
            include_tables=True,
            include_images=True,
            include_links=True,
            include_formatting=True,
            favor_precision=favor_precision_value,
            favor_recall=favor_recall_value,
        )

        if content_string is None:
            raise RuntimeError("Failed in converting.")

        row_data = {
            "title": title,
            "document": TransformUtils.get_file_basename(file_name),
            "contents": content_string,
            "document_id": TransformUtils.str_to_hash(content_string),
            "size": len(content_string),
            "date_acquired": datetime.now().isoformat(),
        }

        return row_data

    def transform_binary(self, file_name: str, byte_array: bytes) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts raw data file (ZIP) / raw HTMLs to Parquet format

        If file_name is detected as a HTML file, it generates a pyarrow table with a single row
        that contains the document converted to a text string.
        If file_name is detected as a ZIP archive, it generates a pyarrow table with a row
        for each HTML file detected in the archive.
        """
        if TransformUtils.get_file_extension(file_name)[1] not in [".zip", ".html"]:
            error_message = f"Unsupported file type: {file_name}. Only ZIP and HTML files are supported."
            logger.error(error_message)
            raise ValueError(error_message)  # Raising an exception with the error message
        data = []
        number_of_rows = 0

        # Process ZIP archive of HTML documents
        if TransformUtils.get_file_extension(file_name)[1] == ".zip":
            with zipfile.ZipFile(io.BytesIO(bytes(byte_array))) as opened_zip:
                # Loop through each file member in the ZIP archive
                for member in opened_zip.infolist():
                    if not member.is_dir() and TransformUtils.get_file_extension(member.filename)[1] == ".html":
                        with opened_zip.open(member) as file:
                            try:
                                # Read the content of the file
                                content_bytes = file.read()

                                row_data = self._convert_html2parquet(
                                    member_filename=member.filename, file_name=file_name, content_bytes=content_bytes
                                )

                                data.append(row_data)
                                number_of_rows += 1
                            except Exception as e:
                                logger.warning(f"Exception {str(e)} processing file {member.filename}, skipping")

        # Process single HTML documents
        elif TransformUtils.get_file_extension(file_name)[1] == ".html":
            try:
                buf = io.BytesIO(bytes(byte_array))
                # Read the content of the HTML file
                content_bytes = buf.read()

                row_data = self._convert_html2parquet(
                    member_filename=None, file_name=file_name, content_bytes=content_bytes
                )

                data.append(row_data)
                number_of_rows += 1

            except Exception as e:
                logger.warning(f"Exception {str(e)} processing file {file_name}, skipping")

        table = pa.Table.from_pylist(data)
        return [(TransformUtils.convert_arrow_to_binary(table=table), ".parquet")], {"nrows": number_of_rows}


logger = get_logger(__name__)

short_name = "html2parquet"
cli_prefix = f"{short_name}_"
html2parquet_output_format_key = f"output_format"
html2parquet_favor_precision_key = f"favor_precision"
html2parquet_favor_recall_key = f"favor_recall"


class html2parquet_output_format(str, enum.Enum):
    MARKDOWN = "markdown"
    TEXT = "txt"

    def __str__(self):
        return str(self.value)


class html2parquet_favor_precision(str, enum.Enum):
    TRUE = True
    FALSE = False

    def __str__(self):
        return str(self.value)


class html2parquet_favor_recall(str, enum.Enum):
    TRUE = True
    FALSE = False

    def __str__(self):
        return str(self.value)


html2parquet_output_format_default = html2parquet_output_format.MARKDOWN
html2parquet_favor_precision_default = html2parquet_favor_precision.TRUE
html2parquet_favor_recall_default = html2parquet_favor_recall.TRUE


html2parquet_output_format_cli_param = f"{cli_prefix}{html2parquet_output_format_key}"
html2parquet_favor_precision_cli_param = f"{cli_prefix}{html2parquet_favor_precision_key}"
html2parquet_favor_recall_cli_param = f"{cli_prefix}{html2parquet_favor_recall_key}"


class Html2ParquetTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=Html2ParquetTransform,
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{html2parquet_output_format_cli_param}",
            type=html2parquet_output_format,
            choices=list(html2parquet_output_format),
            help="Output format for the contents column.",
            default=html2parquet_output_format.MARKDOWN,
        )

        parser.add_argument(
            f"--{html2parquet_favor_precision_cli_param}",
            type=html2parquet_favor_precision,
            choices=list(html2parquet_favor_precision),
            help="Prefers less content but more accurate extraction.",
            default=html2parquet_favor_precision.TRUE,
        )

        parser.add_argument(
            f"--{html2parquet_favor_recall_cli_param}",
            type=html2parquet_favor_recall,
            choices=list(html2parquet_favor_recall),
            help="Extracts more content when uncertain.",
            default=html2parquet_favor_recall.TRUE,
        )

    def apply_input_params(self, args: Namespace) -> bool:
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        logger.info(f"html2parquet parameters are : {self.params}")
        return True
