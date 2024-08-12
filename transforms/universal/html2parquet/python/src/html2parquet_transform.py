import time
from argparse import ArgumentParser, Namespace
from typing import Any
import zipfile
import io
import trafilatura
from datetime import datetime


import pyarrow as pa

# disabled for now
# from data_processing_ray.runtime.ray import RayTransformLauncher
# from data_processing_ray.runtime.ray.runtime_configuration import (
#   RayTransformRuntimeConfiguration,
# )
# import data_processing


from data_processing.transform import AbstractBinaryTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger, TransformUtils


class HtmlToParquetTransform(AbstractBinaryTransform):
    def __init__(self, config: dict[str, Any]):
        pass

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
                if not member.is_dir() and '__MACOSX' not in member.filename:
                    with opened_zip.open(member) as file:
                        try:
                            # Read the content of the file
                            content_bytes = file.read()
                            # Use Trafilatura library
                            content_string = trafilatura.extract(content_bytes)
                            row_data = {
                                "title": member.filename,
                                "document": TransformUtils.get_file_basename(file_name),
                                "contents": content_string,
                                "document_id": TransformUtils.str_to_hash(content_string),
                                "size": len(content_string),
                                "date_acquired": datetime.now().isoformat(),
                            }

                            data.append(row_data)
                            number_of_rows += 1
                        except Exception as e:
                            self.logger.warning(f"Exception {str(e)} processing file {member.filename}, skipping")
        table = pa.Table.from_pylist(data)
        return [(TransformUtils.convert_arrow_to_binary(table=table), ".parquet")], {"number of rows": number_of_rows}

logger = get_logger(__name__)

short_name = "html2parquet"
cli_prefix = f"{short_name}_"


class HtmlToParquetTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=HtmlToParquetTransform,
        )
    def add_input_params(self, parser: ArgumentParser) -> None:
        pass 

    def apply_input_params(self, args: Namespace) -> bool:
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        logger.info(f"html2parquet parameters are : {self.params}")
        return True