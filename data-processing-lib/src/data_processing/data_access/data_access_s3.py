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
from typing import Any

import pyarrow
from data_processing.data_access import ArrowS3, DataAccess
from data_processing.utils import GB, MB, TransformUtils, get_logger


logger = get_logger(__name__)


class DataAccessS3(DataAccess):
    """
    Implementation of the Base Data access class for folder-based data access.
    """

    def __init__(
        self,
        s3_credentials: dict[str, str],
        s3_config: dict[str, str] = None,
        d_sets: list[str] = None,
        checkpoint: bool = False,
        m_files: int = -1,
        n_samples: int = -1,
        files_to_use: list[str] = [".parquet"],
    ):
        """
        Create data access class for folder based configuration
        :param s3_credentials: dictionary of cos credentials
        :param s3_config: dictionary of path info
        :param d_sets list of the data sets to use
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return
        :param n_samples: amount of files to randomly sample
        :param files_to_use: files extensions of files to include
        """
        self.arrS3 = ArrowS3(
            access_key=s3_credentials.get("access_key", ""),
            secret_key=s3_credentials.get("secret_key", ""),
            endpoint=s3_credentials.get("url", None),
            region=s3_credentials.get("region", None),
        )
        if s3_config is None:
            self.input_folder = None
            self.input_folder = None
        else:
            self.input_folder = TransformUtils.clean_path(s3_config["input_folder"])
            self.output_folder = TransformUtils.clean_path(s3_config["output_folder"])
        self.d_sets = d_sets
        self.checkpoint = checkpoint
        self.m_files = m_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use

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

    def _get_files_folder(
        self, path: str, cm_files: int, max_file_size: int = 0, min_file_size: int = MB * GB
    ) -> tuple[list[str], dict[str, float]]:
        """
        Support method to get list input files and their profile
        :param path: input path
        :param max_file_size: max file size
        :param min_file_size: min file size
        :param cm_files: overwrite for the m_files in the class
        :return: tuple of file list and profile
        """
        # Get files list.
        p_list = []
        total_input_file_size = 0
        i = 0
        for file in self.arrS3.list_files(path):
            if i >= cm_files > 0:
                break
            # Only use specified files
            f_name = str(file["name"])
            _, extension = os.path.splitext(f_name)
            if extension in self.files_to_use:
                p_list.append(f_name)
                size = file["size"]
                total_input_file_size += size
                if min_file_size > size:
                    min_file_size = size
                if max_file_size < size:
                    max_file_size = size
                i += 1
        return (
            p_list,
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
        :param cm_files: max files to get
        :return: tuple of file list and and profile
        """
        if not self.checkpoint:
            return self._get_files_folder(
                path=input_path, cm_files=cm_files, min_file_size=min_file_size, max_file_size=max_file_size
            )
        pout_list, _ = self._get_files_folder(path=output_path, cm_files=-1)
        output_base_names = [file.replace(self.output_folder, self.input_folder) for file in pout_list]
        p_list = []
        total_input_file_size = 0
        i = 0
        for file in self.arrS3.list_files(input_path):
            if i >= cm_files > 0:
                break
            # Only use .parquet files
            f_name = str(file["name"])
            _, extension = os.path.splitext(f_name)
            if extension in self.files_to_use and f_name not in output_base_names:
                p_list.append(f_name)
                size = file["size"]
                total_input_file_size += size
                if min_file_size > size:
                    min_file_size = size
                if max_file_size < size:
                    max_file_size = size
                i += 1
        return (
            p_list,
            {
                "max_file_size": max_file_size / MB,
                "min_file_size": min_file_size / MB,
                "total_file_size": total_input_file_size / MB,
            },
        )

    def get_files_to_process_internal(self) -> tuple[list[str], dict[str, float]]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size",
            "min_file_size",
            "total_file_size"
        """
        if self.output_folder is None:
            logger.error("Get files to process. S3 configuration is not present, returning empty")
            return [], {}
        # Check if we are using data sets
        if self.d_sets is not None:
            # get folders for the input
            folders_to_use = []
            folders = self.arrS3.list_folders(self.input_folder)
            # Only use valid folders
            for ds in self.d_sets:
                suffix = ds + "/"
                for f in folders:
                    if f.endswith(suffix):
                        folders_to_use.append(f)
                        break
            profile = {"max_file_size": 0.0, "min_file_size": 0.0, "total_file_size": 0.0}
            if len(folders_to_use) > 0:
                # if we have valid folders
                path_list = []
                max_file_size = 0
                min_file_size = MB * GB
                total_file_size = 0
                cm_files = self.m_files
                for folder in folders_to_use:
                    plist, profile = self._get_input_files(
                        input_path=self.input_folder + folder,
                        output_path=self.output_folder + folder,
                        cm_files=cm_files,
                        min_file_size=min_file_size,
                        max_file_size=max_file_size,
                    )
                    path_list += plist
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
        return path_list, profile

    def get_table(self, path: str) -> pyarrow.table:
        """
        Get pyArrow table for a given path
        :param path - file path
        :return: pyArrow table or None, if the table read failed
        """
        return self.arrS3.read_table(path)

    def get_output_location(self, path: str) -> str:
        """
        Get output location based on input
        :param path: input file location
        :return: output file location
        """
        if self.output_folder is None:
            logger.error("Get out put location. S3 configuration is not provided, returning None")
            return None
        return path.replace(self.input_folder, self.output_folder)

    def save_table(self, path: str, table: pyarrow.Table) -> tuple[int, dict[str, Any]]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        return self.arrS3.save_table(key=path, table=table)

    def save_job_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
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
            logger.error("S3 configuration is not provided, can't save metadata")
            return None
        metadata["source"] = {"name": self.input_folder, "type": "path"}
        metadata["target"] = {"name": self.output_folder, "type": "path"}
        return self.save_file(path=f"{self.output_folder}metadata.json", data=json.dumps(metadata, indent=2).encode())

    def get_file(self, path: str) -> bytes:
        """
        Get file as a byte array
        :param path: file path
        :return: bytes array of file content
        """
        filedata = self.arrS3.read_file(path)
        if path.endswith("gz"):
            filedata = gzip.decompress(filedata)
        return filedata

    def get_folder_files(self, path: str, extensions: list[str] = None, return_data: bool = True) -> dict[str, bytes]:
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

        def _get_file_content(name: str, dt: bool) -> bytes:
            """
            return file content
            :param name: file name
            :param dt: flag to return data or None
            :return: file content
            """
            if dt:
                return self.get_file(name)
            return None

        result = {}
        files = self.arrS3.list_files(key=TransformUtils.clean_path(path))
        for file in files:
            f_name = str(file["name"])
            if extensions is None:
                if not f_name.endswith("/"):
                    # skip folders
                    result[f_name] = _get_file_content(f_name, return_data)
            else:
                for ext in extensions:
                    if f_name.endswith(ext):
                        # include the file
                        result[f_name] = _get_file_content(f_name, return_data)
                        break
        return result

    def save_file(self, path: str, data: bytes) -> dict[str, Any]:
        """
        Save byte array to the file
        :param path: file path
        :param data: byte array
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        return self.arrS3.save_file(key=path, data=data)
