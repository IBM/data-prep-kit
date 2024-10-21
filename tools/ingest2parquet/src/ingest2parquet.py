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

import argparse
import io
import os
import uuid
import zipfile
from datetime import datetime
from multiprocessing import Pool
from typing import Any

import pandas as pd
import pyarrow as pa
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.utils import TransformUtils
from utils import detect_language


def zip_to_table(data_access: DataAccess, file_path, detect_prog_lang: Any) -> pa.table:
    """
    Extracts contents from a ZIP file and converts them into a PyArrow table.

    :param data_access: DataAccess object for accessing data
    :param file_path: Path to the ZIP file
    :return: PyArrow table containing extracted data
    """
    data = []
    zip_name = os.path.basename(file_path)
    compressed_zip_bytes, _ = data_access.get_file(file_path)

    with zipfile.ZipFile(io.BytesIO(bytes(compressed_zip_bytes))) as opened_zip:
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
                                "document": zip_name,
                                "contents": content_string,
                                "document_id": str(uuid.uuid4()),
                                "ext": ext,
                                "hash": TransformUtils.str_to_hash(content_string),
                                "size": TransformUtils.deep_get_size(content_string),
                                "date_acquired": datetime.now().isoformat(),
                                "repo_name": os.path.splitext(os.path.basename(zip_name))[0],
                            }
                            if detect_prog_lang:
                                lang = detect_prog_lang.get_lang_from_ext(ext)
                                row_data["programming_language"] = lang

                            data.append(row_data)
                        else:
                            raise Exception("No contents decoded")

                    except Exception as e:
                        print(f" skipping {member.filename} {str(e)}")
    table = pa.Table.from_pylist(data)
    return table


def raw_to_parquet(
    data_access_factory: DataAccessFactory,
    file_path,
    detect_prog_lang: Any,
    snapshot: str,
    domain: str,
) -> tuple[bool, dict[str:Any]]:
    """
    Converts raw data file (ZIP) to Parquet format and saves it.

    :param data_access_factory: DataAccessFactory object for accessing data
    :param file_path: Path to the raw data file
    :return: Tuple indicating success (True/False) and additional metadata
    """

    try:
        # Create a DataAccess object for accessing data
        data_access = data_access_factory.create_data_access()

        # Get the file extension
        ext = TransformUtils.get_file_extension(file_path)[1]
        if ext == ".zip":
            table = zip_to_table(data_access, file_path, detect_prog_lang)
            if table.num_rows > 0:
                snapshot_column = [snapshot] * table.num_rows
                table = TransformUtils.add_column(table=table, name="snapshot", content=snapshot_column)
                domain_column = [domain] * table.num_rows
                table = TransformUtils.add_column(table=table, name="domain", content=domain_column)
        else:
            raise Exception(f"Got {ext} file, not supported")
        if table.num_rows > 0:
            # Get the output file name for the Parquet file
            output_file_name = data_access.get_output_location(file_path).replace(".zip", ".parquet")
            # Save the PyArrow table as a Parquet file and get metadata
            l, metadata, _ = data_access.save_table(output_file_name, table)
            if metadata:
                return (
                    True,
                    {
                        "path": file_path,
                        "bytes_in_memory": l,
                        "row_count": table.num_rows,
                    },
                )
            else:
                raise Exception("Failed to upload")

    except Exception as e:
        return False, {"path": file_path, "error": str(e)}


def generate_stats(metadata: list) -> dict[str, Any]:
    """
    Generates statistics based on processing metadata.

    :param metadata: List of tuples containing processing metadata
    :return: Dictionary containing processing statistics
    """
    success = 0
    success_details = []
    failures = 0
    failure_details = []
    for m in metadata:
        if m[0] == True:
            success += 1
            success_details.append(m[1])
        else:
            failures += 1
            failure_details.append(m[1])

    # Create DataFrame from success details to calculate total bytes in memory
    success_df = pd.DataFrame(success_details, columns=["path", "bytes_in_memory", "row_count"])
    total_bytes_in_memory = success_df["bytes_in_memory"].sum()
    total_row_count = success_df["row_count"].sum()

    return {
        "total_files_given": len(metadata),
        "total_files_processed": success,
        "total_files_failed_to_processed": failures,
        "total_no_of_rows": int(total_row_count),
        "total_bytes_in_memory": int(total_bytes_in_memory),
        "failure_details": failure_details,
    }


def run():
    """
    Deprecated in favor of ingest2parquet().
    """
    print("Warning: run() method is deprecated in favor of ingest2parquet()")
    return ingest2parquet()


def ingest2parquet():
    parser = argparse.ArgumentParser(description="raw-data-to-parquet")
    parser.add_argument(
        "-detect_programming_lang",
        "--detect_programming_lang",
        type=bool,
        help="generate programming lang",
    )
    parser.add_argument("-snapshot", "--snapshot", type=str, help="Name the dataset", default="")
    parser.add_argument(
        "-domain",
        "--domain",
        type=str,
        help="To identify whether data is code or natural language",
        default="",
    )

    data_access_factory = DataAccessFactory()
    data_access_factory.add_input_params(parser)

    args = parser.parse_args()
    data_access_factory.apply_input_params(args)

    # Creates a DataAccess object for accessing data.
    data_access = data_access_factory.create_data_access()

    # Retrieves file paths of files from the input folder.
    file_paths, _ = data_access.get_folder_files(data_access.input_folder, args.data_files_to_use, False)

    if len(file_paths) != 0:
        print(f"Number of files is {len(file_paths)} ")
        detect_prog_lang = detect_language.Detect_Programming_Lang() if args.detect_programming_lang else None

        with Pool() as p:
            # Processes each file concurrently
            results = p.starmap_async(
                raw_to_parquet,
                [
                    (
                        data_access_factory,
                        file_path,
                        detect_prog_lang,
                        args.snapshot,
                        args.domain,
                    )
                    for file_path in file_paths.keys()
                ],
            )
            metadata = results.get()
        # Generates statistics based on the processing metadata
        stats = generate_stats(metadata)
        print("processing stats generated", stats)

        # Saves the processing statistics
        res, _ = data_access.save_job_metadata(stats)
        print("Metadata file stored - response:", res)


if __name__ == "__main__":
    ingest2parquet()
