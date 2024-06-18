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

import gzip
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from data_processing.data_access import DataAccess
from data_processing.utils import GB, MB, TransformUtils, get_logger


logger = get_logger(__name__)


class DataAccessLocal(DataAccess):
    """
    Implementation of the Base Data access class for local folder data access.
    """

    def __init__(
        self,
        local_config: dict[str, str] = None,
        d_sets: list[str] = None,
        checkpoint: bool = False,
        m_files: int = -1,
        n_samples: int = -1,
        files_to_use: list[str] = [".parquet"],
        files_to_checkpoint: list[str] = [".parquet"],
    ):
        """
        Create data access class for folder based configuration
        :param local_config: dictionary of path info
        :param d_sets list of the data sets to use
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return
        :param n_samples: amount of files to randomly sample
        :param files_to_use: files extensions of files to include
        :param files_to_checkpoint: files extensions of files to use for checkpointing
        """
        if local_config is None:
            self.input_folder = None
            self.output_folder = None
        else:
            self.input_folder = local_config["input_folder"]
            self.output_folder = local_config["output_folder"]
        self.d_sets = d_sets
        self.checkpoint = checkpoint
        self.m_files = m_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use
        self.files_to_checkpoint = files_to_checkpoint

        logger.debug(f"Local input folder: {self.input_folder}")
        logger.debug(f"Local output folder: {self.output_folder}")
        logger.debug(f"Local data sets: {self.d_sets}")
        logger.debug(f"Local checkpoint: {self.checkpoint}")
        logger.debug(f"Local m_files: {self.m_files}")
        logger.debug(f"Local n_samples: {self.n_samples}")
        logger.debug(f"Local files_to_use: {self.files_to_use}")
        logger.debug(f"Local files_to_checkpoint: {self.files_to_checkpoint}")

    def get_num_samples(self) -> int:
        """
        Get number of samples for input
        :return: Number of samples
        """
        return self.n_samples

    def get_output_folder(self) -> str:
        """
        Get output folder as a string
        :return: output_folder
        """
        return self.output_folder

    @staticmethod
    def _get_all_files_ext(path: str, extensions: list[str]) -> list[str]:
        """
        Get files with the given extension for a given folder and all sub folders
        :param path: starting path
        :param extensions: List of extensions, None - all
        :return: List of files
        """
        files = []
        for c_path in sorted(Path(path).rglob("*")):
            # for every file
            if c_path.is_dir():
                continue
            s_path = str(c_path.absolute())
            if extensions is not None:
                _, extension = os.path.splitext(s_path)
                if extension not in extensions:
                    continue
            files.append(s_path)
        return files

    def _get_files_folder(
        self, path: str, cm_files: int, max_file_size: int = 0, min_file_size: int = MB * GB
    ) -> tuple[list[str], dict[str, float]]:
        """
        Support method.  Lists all parquet files in a directory and their sizes.
        :param path: input path
        :param max_file_size: max file size, not sure
        :param min_file_size: min file size
        :param cm_files: overwrite for the m_files in the class
        :return: tuple of file list and profile
        """
        # Get files list, their total size, max and min size of the files in the list.
        result_files = []
        total_input_file_size = 0
        i = 0
        for c_path in sorted(self._get_all_files_ext(path=path, extensions=self.files_to_use)):
            if i >= cm_files > 0:
                break
            size = Path(c_path).stat().st_size
            result_files.append(c_path)
            total_input_file_size += size
            if min_file_size > size:
                min_file_size = size
            if max_file_size < size:
                max_file_size = size
            i += 1
        return (
            result_files,
            {
                "max_file_size": max_file_size / MB,
                "min_file_size": min_file_size / MB,
                "total_file_size": total_input_file_size / MB,
            },
        )

    def _get_input_files(
        self,
        input_path: str,
        output_path: str,
        cm_files: int,
        max_file_size: int = 0,
        min_file_size: int = MB * GB,
    ) -> tuple[list[str], dict[str, float]]:
        """
        Get list and size of files from input path, that do not exist in the output path
        :param input_path: input path
        :param output_path: output path
        :return: tuple of file list and profile
        """
        if not self.checkpoint:
            return self._get_files_folder(
                path=input_path, cm_files=cm_files, min_file_size=min_file_size, max_file_size=max_file_size
            )

        input_files = self._get_all_files_ext(path=input_path, extensions=self.files_to_use)
        output_files_ext = self._get_all_files_ext(path=output_path, extensions=self.files_to_checkpoint)
        # In the case of binary transforms, an extension can be different, so just use the file names.
        # Also remove duplicates
        output_files = list(set([TransformUtils.get_file_extension(file)[0] for file in output_files_ext]))

        total_input_file_size = 0
        i = 0
        result_files = []
        for filename in sorted(input_files):
            out_f_name = self.get_output_location(filename)
            if TransformUtils.get_file_extension(out_f_name)[0] in output_files:
                continue
            if i >= cm_files > 0:
                break
            result_files.append(filename)
            size = os.path.getsize(os.path.join(input_path, filename))
            total_input_file_size += size
            if min_file_size > size:
                min_file_size = size
            if max_file_size < size:
                max_file_size = size
            i += 1
        return (
            result_files,
            {
                "max_file_size": max_file_size / MB,
                "min_file_size": min_file_size / MB,
                "total_file_size": total_input_file_size / MB,
            },
        )

    def get_files_to_process_internal(self) -> tuple[list[str], dict[str, float], int]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size",
            "min_file_size",
            "total_file_size"
        """
        if self.output_folder is None:
            logger.error("Get files to process. local configuration is not present, returning empty")
            return [], {}, 0
        # Check if we are using data sets
        if self.d_sets is not None:
            # get a list of subdirectory paths matching d_sets
            folders_to_use = []
            root_dir = Path(self.input_folder)
            for dir_name in self.d_sets:
                subdir_path = root_dir / dir_name
                if subdir_path.is_dir() and subdir_path.parent == root_dir:
                    folders_to_use.append(subdir_path.name)
            profile = {
                "max_file_size": 0.0,
                "min_file_size": 0.0,
                "total_file_size": 0.0,
            }
            if len(folders_to_use) > 0:
                # if we have valid folders
                path_list = []
                max_file_size = 0
                min_file_size = MB * GB
                total_file_size = 0
                cm_files = self.m_files
                for folder in folders_to_use:
                    plist, profile = self._get_input_files(
                        input_path=os.path.join(self.input_folder, folder),
                        output_path=os.path.join(self.output_folder, folder),
                        cm_files=cm_files,
                        min_file_size=min_file_size,
                        max_file_size=max_file_size,
                    )
                    path_list += [os.path.join(self.input_folder, folder, x) for x in plist]
                    total_file_size += profile["total_file_size"]
                    if len(path_list) >= cm_files > 0:
                        break
                    max_file_size = profile["max_file_size"] * MB
                    min_file_size = profile["min_file_size"] * MB
                    if cm_files > 0:
                        cm_files -= len(plist)
                profile["total_file_size"] = total_file_size
            else:
                path_list = []
        else:
            # Get input files list
            path_list, profile = self._get_input_files(
                input_path=self.input_folder,
                output_path=self.output_folder,
                cm_files=self.m_files,
            )
        return path_list, profile, 0

    def get_table(self, path: str) -> tuple[pa.table, int]:
        """
        Attempts to read a PyArrow table from the given path.

        Args:
            path (str): Path to the file containing the table.

        Returns:
            pyarrow.Table: PyArrow table if read successfully, None otherwise.
        """

        try:
            table = pq.read_table(path)
            return table, 0
        except (FileNotFoundError, IOError, pa.ArrowException) as e:
            logger.error(f"Error reading table from {path}: {e}")
            return None, 0

    def get_output_location(self, path: str) -> str:
        """
        Get output location based on input
        :param path: input file location
        :return: output file location
        """
        if self.output_folder is None:
            logger.error("Get output location. local configuration is not defined, returning None")
            return None
        return path.replace(self.input_folder, self.output_folder)

    def save_table(self, path: str, table: pa.Table) -> tuple[int, dict[str, Any], int]:
        """
        Saves a pyarrow table to a file and returns information about the operation.

        Args:
            table (pyarrow.Table): The pyarrow table to save.
            path (str): The path to the output file.

        Returns:
            tuple: A tuple containing:
                - size_in_memory (int): The size of the table in memory (bytes).
                - file_info (dict or None): A dictionary containing:
                    - name (str): The name of the file.
                    - size (int): The size of the file (bytes).
                If saving fails, file_info will be None.
        """
        # Get table size in memory
        size_in_memory = table.nbytes
        try:
            # Write the table to parquet format
            os.makedirs(os.path.dirname(path), exist_ok=True)
            pq.write_table(table, path)

            # Get file size and create file_info
            file_info = {"name": os.path.basename(path), "size": os.path.getsize(path)}
            return size_in_memory, file_info, 0

        except Exception as e:
            logger.error(f"Error saving table to {path}: {e}")
            return -1, None, 0

    def save_job_metadata(self, metadata: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """
        Save metadata
        :param metadata: a dictionary, containing the following keys
        (see https://github.ibm.com/arc/dmf-library/issues/158):
            "pipeline",
            "job details",
            "code",
            "job_input_params",
            "execution_stats",
            "job_output_stats"
        two additional elements:
            "source"
            "target"
        are filled bu implementation
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        if self.output_folder is None:
            logger.error("local configuration is not defined, can't save metadata")
            return None, 0
        metadata["source"] = {"name": self.input_folder, "type": "path"}
        metadata["target"] = {"name": self.output_folder, "type": "path"}
        return self.save_file(
            path=os.path.join(self.output_folder, "metadata.json"),
            data=json.dumps(metadata, indent=2).encode(),
        )

    def get_file(self, path: str) -> tuple[bytes, int]:
        """
        Gets the contents of a file as a byte array, decompressing gz files if needed.

        Args:
            path (str): The path to the file.

        Returns:
            bytes: The contents of the file as a byte array, or None if an error occurs.
        """

        try:
            if path.endswith(".gz"):
                with gzip.open(path, "rb") as f:
                    data = f.read()
            else:
                with open(path, "rb") as f:
                    data = f.read()
            return data, 0

        except (FileNotFoundError, gzip.BadGzipFile) as e:
            logger.error(f"Error reading file {path}: {e}")
            raise e

    def get_folder_files(
        self, path: str, extensions: list[str] = None, return_data: bool = True
    ) -> tuple[dict[str, bytes], int]:
        """
        Get a list of byte content of files. The path here is an absolute path and can be anywhere.
        The current limitation for S3 and Lakehouse is that it has to be in the same bucket
        :param path: file path
        :param extensions: a list of file extensions to include. If None, then all files from this and
                           child ones will be returned
        :param return_data: flag specifying whether the actual content of files is returned (True), or just
                            directory is returned (False)
        :return: A dictionary of file names/binary content will be returned
        """

        def _get_file_content(f_name: str, dt: bool) -> tuple[bytes, int]:
            """
            return file content
            :param f_name: file name
            :param dt: flag to return data or None
            :return: file content
            """
            if dt:
                return self.get_file(f_name)
            return None, 0

        matching_files = {}
        for filename in sorted(self._get_all_files_ext(path=path, extensions=extensions)):
            b, _ = _get_file_content(filename, return_data)
            matching_files[filename] = b
        return matching_files, 0

    def save_file(self, path: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Saves bytes to a file and returns a dictionary with file information.

        Args:
            data (bytes): The bytes data to save.
            path (str): The full name of the file to save.

        Returns:
            dict or None: A dictionary with "name" and "size" keys if successful,
                        or None if saving fails.
        """

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            file_info = {"name": path, "size": os.path.getsize(path)}
            return file_info, 0

        except Exception as e:
            logger.error(f"Error saving bytes to file {path}: {e}")
            return None, 0
