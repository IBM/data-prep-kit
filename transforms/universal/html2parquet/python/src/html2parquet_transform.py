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


class Html2ParquetTransform(AbstractBinaryTransform):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)  

    def _convert_html2parquet(self, member_filename:str, file_name:str, content_bytes: bytes) -> dict:
        title = member_filename if member_filename else TransformUtils.get_file_basename(file_name)
        
        # Use Trafilatura library
        content_string = trafilatura.extract(content_bytes)

        if content_string is None:
            raise RuntimeError("Failed in converting.")

        row_data = {
            "title": title,
            "document": TransformUtils.get_file_basename(file_name),
            "contents": content_string,
            "document_id": TransformUtils.str_to_hash(content_string),
            "size": len(content_string),
            "date_acquired": datetime.now().isoformat()
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
            self.logger.error(error_message)
            raise ValueError(error_message)  # Raising an exception with the error message
        data = []
        number_of_rows = 0

        # Process ZIP archive of HTML documents
        if(TransformUtils.get_file_extension(file_name)[1] == ".zip"):
            with zipfile.ZipFile(io.BytesIO(bytes(byte_array))) as opened_zip:
                # Loop through each file member in the ZIP archive
                for member in opened_zip.infolist():
                    if not member.is_dir() and TransformUtils.get_file_extension(member.filename)[1] == ".html":
                        with opened_zip.open(member) as file:
                            try:
                                # Read the content of the file
                                content_bytes = file.read()

                                row_data = self._convert_html2parquet(member_filename=member.filename ,file_name=file_name, content_bytes=content_bytes)

                                data.append(row_data)
                                number_of_rows += 1
                            except Exception as e:
                                self.logger.warning(f"Exception {str(e)} processing file {member.filename}, skipping")
            
  
        # Process single HTML documents
        elif(TransformUtils.get_file_extension(file_name)[1] == ".html"):
            try:
                buf = io.BytesIO(bytes(byte_array))
                # Read the content of the HTML file
                content_bytes = buf.read()

                row_data = self._convert_html2parquet(member_filename=None ,file_name=file_name, content_bytes=content_bytes)

                data.append(row_data)
                number_of_rows += 1

            except Exception as e:
                self.logger.warning(f"Exception {str(e)} processing file {file_name}, skipping")
            

        table = pa.Table.from_pylist(data)
        return [(TransformUtils.convert_arrow_to_binary(table=table), ".parquet")], {"number of rows": number_of_rows}

logger = get_logger(__name__)

short_name = "html2parquet"
cli_prefix = f"{short_name}_"


class Html2ParquetTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=Html2ParquetTransform,
        )
    def add_input_params(self, parser: ArgumentParser) -> None:
        pass 

    def apply_input_params(self, args: Namespace) -> bool:
        return True