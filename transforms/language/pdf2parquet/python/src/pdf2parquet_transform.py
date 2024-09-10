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
import json
import time
import uuid
import zipfile
from argparse import ArgumentParser, Namespace
from datetime import datetime
from pathlib import Path
from typing import Any

import filetype
import pandas as pd
import pyarrow as pa
from data_processing.transform import AbstractBinaryTransform, TransformConfiguration
from data_processing.utils import TransformUtils, get_logger, str2bool
from data_processing.utils.cli_utils import CLIArgumentProvider
from docling.datamodel.base_models import DocumentStream
from docling.datamodel.document import ConvertedDocument, DocumentConversionInput
from docling.document_converter import DocumentConverter
from docling.pipeline.standard_model_pipeline import PipelineOptions


logger = get_logger(__name__)

shortname = "pdf2parquet"
cli_prefix = f"{shortname}_"
pdf2parquet_artifacts_path_key = f"artifacts_path"
pdf2parquet_contents_type_key = f"contents_type"
pdf2parquet_do_table_structure_key = f"do_table_structure"
pdf2parquet_do_ocr_key = f"do_ocr"
pdf2parquet_double_precision_key = f"double_precision"


class pdf2parquet_contents_types(str, enum.Enum):
    MARKDOWN = "text/markdown"
    JSON = "application/json"

    def __str__(self):
        return str(self.value)


pdf2parquet_contents_type_default = pdf2parquet_contents_types.MARKDOWN
pdf2parquet_do_table_structure_default = True
pdf2parquet_do_ocr_default = True
pdf2parquet_double_precision_default = 8

pdf2parquet_artifacts_path_cli_param = f"{cli_prefix}{pdf2parquet_artifacts_path_key}"
pdf2parquet_contents_type_cli_param = f"{cli_prefix}{pdf2parquet_contents_type_key}"
pdf2parquet_do_table_structure_cli_param = (
    f"{cli_prefix}{pdf2parquet_do_table_structure_key}"
)
pdf2parquet_do_ocr_cli_param = f"{cli_prefix}{pdf2parquet_do_ocr_key}"
pdf2parquet_double_precision_cli_param = (
    f"{cli_prefix}{pdf2parquet_double_precision_key}"
)


class Pdf2ParquetTransform(AbstractBinaryTransform):
    """ """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, LangSelectorTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """

        super().__init__(config)

        self.artifacts_path = config.get(pdf2parquet_artifacts_path_key, None)
        if self.artifacts_path is not None:
            self.artifacts_path = Path(self.artifacts_path)
        self.contents_type = config.get(
            pdf2parquet_contents_type_key, pdf2parquet_contents_types.MARKDOWN
        )
        if not isinstance(self.contents_type, pdf2parquet_contents_types):
            self.contents_type = pdf2parquet_contents_types[self.contents_type]
        self.do_table_structure = config.get(
            pdf2parquet_do_table_structure_key, pdf2parquet_do_table_structure_default
        )
        self.do_ocr = config.get(pdf2parquet_do_ocr_key, pdf2parquet_do_ocr_default)
        self.double_precision = config.get(
            pdf2parquet_double_precision_key, pdf2parquet_double_precision_default
        )

        logger.info("Initializing models")
        pipeline_options = PipelineOptions(
            do_table_structure=self.do_table_structure,
            do_ocr=self.do_ocr,
        )
        self._converter = DocumentConverter(
            artifacts_path=self.artifacts_path, pipeline_options=pipeline_options
        )

    def _update_metrics(self, num_pages: int, elapse_time: float):
        # This is implemented in the ray version
        pass

    def _convert_pdf2parquet(
        self, doc_filename: str, ext: str, content_bytes: bytes
    ) -> dict:
        # Convert PDF to Markdown
        start_time = time.time()
        buf = io.BytesIO(content_bytes)
        input_docs = DocumentStream(filename=doc_filename, stream=buf)
        input = DocumentConversionInput.from_streams([input_docs])

        converted_docs = self._converter.convert(input)
        doc: ConvertedDocument = next(converted_docs, None)
        if doc is None or doc.output is None:
            raise RuntimeError("Failed in converting.")
        elapse_time = time.time() - start_time

        if self.contents_type == pdf2parquet_contents_types.MARKDOWN:
            content_string = doc.render_as_markdown()
        elif self.contents_type == pdf2parquet_contents_types.JSON:
            content_string = pd.io.json.ujson_dumps(
                doc.render_as_dict(), double_precision=self.double_precision
            )
        else:
            raise RuntimeError(f"Uknown contents_type {self.contents_type}.")
        num_pages = len(doc.pages)
        num_tables = len(doc.output.tables) if doc.output.tables is not None else 0
        num_doc_elements = len(doc.output.main_text) if doc.output.main_text is not None else 0

        self._update_metrics(num_pages=num_pages, elapse_time=elapse_time)

        file_data = {
            "filename": TransformUtils.get_file_basename(doc_filename),
            "contents": content_string,
            "num_pages": num_pages,
            "num_tables": num_tables,
            "num_doc_elements": num_doc_elements,
            "document_id": str(uuid.uuid4()),
            "ext": ext,
            "hash": TransformUtils.str_to_hash(content_string),
            "size": len(content_string),
            "date_acquired": datetime.now().isoformat(),
            "pdf_convert_time": elapse_time,
        }

        return file_data

    def transform_binary(
        self, file_name: str, byte_array: bytes
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        If file_name is detected as a PDF file, it generates a pyarrow table with a single row
        containing the document converted in markdown format.
        If file_name is detected as a ZIP archive, it generates a pyarrow table with a row
        for each PDF file detected in the archive.
        """

        data = []
        success_doc_id = []
        failed_doc_id = []
        skipped_doc_id = []
        number_of_rows = 0

        try:
            root_kind = filetype.guess(byte_array)

            # Process single PDF documents
            if root_kind is not None and root_kind.mime == "application/pdf":
                logger.debug(f"Detected root file {file_name=} as PDF.")

                try:
                    root_ext = root_kind.extension
                    file_data = self._convert_pdf2parquet(
                        doc_filename=file_name, ext=root_ext, content_bytes=byte_array
                    )

                    file_data["source_filename"] = TransformUtils.get_file_basename(
                        file_name
                    )

                    data.append(file_data)
                    number_of_rows += 1
                    success_doc_id.append(file_name)

                except Exception as e:
                    failed_doc_id.append(file_name)
                    logger.warning(
                        f"Exception {str(e)} processing file {archive_doc_filename}, skipping"
                    )

            # Process ZIP archive of PDF documents
            elif root_kind is not None and root_kind.mime == "application/zip":
                logger.debug(
                    f"Detected root file {file_name=} as ZIP. Iterating through the archive content."
                )

                with zipfile.ZipFile(io.BytesIO(byte_array)) as opened_zip:
                    zip_namelist = opened_zip.namelist()

                    for archive_doc_filename in zip_namelist:

                        logger.info("Processing " f"{archive_doc_filename=} ")

                        with opened_zip.open(archive_doc_filename) as file:
                            try:
                                # Read the content of the file
                                content_bytes = file.read()

                                # Detect file type
                                kind = filetype.guess(content_bytes)
                                if kind is None or kind.mime != "application/pdf":
                                    logger.info(
                                        f"File {archive_doc_filename=} is not detected as PDF but {kind=}. Skipping."
                                    )
                                    skipped_doc_id.append(archive_doc_filename)
                                    continue

                                ext = kind.extension

                                file_data = self._convert_pdf2parquet(
                                    doc_filename=archive_doc_filename,
                                    ext=ext,
                                    content_bytes=content_bytes,
                                )
                                file_data["source_filename"] = (
                                    TransformUtils.get_file_basename(file_name)
                                )

                                data.append(file_data)
                                success_doc_id.append(archive_doc_filename)
                                number_of_rows += 1

                            except Exception as e:
                                failed_doc_id.append(archive_doc_filename)
                                logger.warning(
                                    f"Exception {str(e)} processing file {archive_doc_filename}, skipping"
                                )

            else:
                logger.warning(
                    f"File {file_name=} is not detected as PDF nor as ZIP but {kind=}. Skipping."
                )

            table = pa.Table.from_pylist(data)
            metadata = {
                "nrows": len(table),
                "nsuccess": len(success_doc_id),
                "nfail": len(failed_doc_id),
                "nskip": len(skipped_doc_id),
            }
            return [
                (TransformUtils.convert_arrow_to_binary(table=table), ".parquet")
            ], metadata
        except Exception as e:
            logger.error(f"Fatal error with file {file_name=}. No results produced.")
            raise


class Pdf2ParquetTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(
        self, transform_class: type[AbstractBinaryTransform] = Pdf2ParquetTransform
    ):
        super().__init__(
            name=shortname,
            transform_class=transform_class,
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{pdf2parquet_artifacts_path_cli_param}",
            type=str,
            help="Path where to models artifacts are located, if unset they will be downloaded in the HF_CACHE folder.",
            default=None,
        )
        parser.add_argument(
            f"--{pdf2parquet_contents_type_cli_param}",
            type=pdf2parquet_contents_types,
            choices=list(pdf2parquet_contents_types),
            help="Content type to use for the contents column.",
            default=pdf2parquet_contents_types.MARKDOWN,
        )
        parser.add_argument(
            f"--{pdf2parquet_do_table_structure_cli_param}",
            type=str2bool,
            help="If true, detected tables will be processed with the table structure model.",
            default=pdf2parquet_do_table_structure_default,
        )
        parser.add_argument(
            f"--{pdf2parquet_do_ocr_cli_param}",
            type=str2bool,
            help="If true, optical character recognition (OCR) will be used to read the PDF content.",
            default=pdf2parquet_do_ocr_default,
        )
        parser.add_argument(
            f"--{pdf2parquet_double_precision_cli_param}",
            type=int,
            required=False,
            help="If set, all floating points (e.g. bounding boxes) are rounded to this precision. For tests it is advised to use 0.",
            default=pdf2parquet_double_precision_default,
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """

        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        logger.info(f"pdf2parquet parameters are : {self.params}")
        return True
