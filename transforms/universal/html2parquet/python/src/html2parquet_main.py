import time
from argparse import ArgumentParser, Namespace
from typing import Any
import zipfile
import io
import trafilatura
from datetime import datetime


import pyarrow as pa

#distabled for now
# from data_processing_ray.runtime.ray import RayTransformLauncher
# from data_processing_ray.runtime.ray.runtime_configuration import (
#   RayTransformRuntimeConfiguration,
# )
# import data_processing


from data_processing.transform import AbstractTableTransform, AbstractBinaryTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger, TransformUtils


class HtmlToParquetTransform(AbstractTableTransform):
    def __init__(self, config: dict[str, Any]):
        self.sleep = config.get("sleep", 1)

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
                            #use Tra something library
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


 
    # def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
    #     if self.sleep is not None:
    #         time.sleep(self.sleep)
    #     # Add some sample metadata.
    #     metadata = {"nfiles": 1, "nrows": len(table)}
    #     return [table], metadata


short_name = "html2parquet"
cli_prefix = f"{short_name}_"
sleep_key = "sleep_sec"
pwd_key = "pwd"
sleep_cli_param = f"{cli_prefix}{sleep_key}"
pwd_cli_param = f"{cli_prefix}{pwd_key}"


class HtmlToParquetTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=HtmlToParquetTransform,
            remove_from_metadata=[pwd_key],
        )
    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{sleep_cli_param}",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )
        parser.add_argument(
            f"--{pwd_cli_param}",
            type=str,
            default="nothing",
            help="A dummy password which should be filtered out of the metadata",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(sleep_key) < 0:
            print(f"Parameter html2parquet_sleep_sec should be non-negative. you specified {args.html2parquet_sleep_sec}")
            return False
        self.params = captured
        return True
from data_processing.runtime.pure_python import PythonTransformLauncher
if __name__ == "__main__":
    launcher = PythonTransformLauncher(runtime_config=HtmlToParquetTransformConfiguration())
    launcher.launch()