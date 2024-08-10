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
        files_to_checkpoint: list[str] = [".parquet"],
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
        :param files_to_checkpoint: files extensions of files to use for checkpointing
        """
        if (
            s3_credentials is None
            or s3_credentials.get("access_key", None) is None
            or s3_credentials.get("secret_key", None) is None
        ):
            raise "S3 credentials is not defined"
        self.s3_credentials = s3_credentials
        if s3_config is None:
            self.input_folder = None
            self.output_folder = None
        else:
            self.input_folder = TransformUtils.clean_path(s3_config["input_folder"])
            self.output_folder = TransformUtils.clean_path(s3_config["output_folder"])
        self.d_sets = d_sets
        self.checkpoint = checkpoint
        self.m_files = m_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use
        self.files_to_checkpoint = files_to_checkpoint
        self.arrS3 = ArrowS3(
            access_key=s3_credentials.get("access_key"),
            secret_key=s3_credentials.get("secret_key"),
            endpoint=s3_credentials.get("url", None),
            region=s3_credentials.get("region", None),
        )

    def get_access_key(self):
        return self.s3_credentials.get("access_key", None)

    def get_secret_key(self):
        return self.s3_credentials.get("secret_key", None)

    def get_endpoint(self):
        return self.s3_credentials.get("url", None)

    def get_region(self):
        return self.s3_credentials.get("region", None)

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
        self, path: str, files_to_use: list[str], cm_files: int, max_file_size: int = 0, min_file_size: int = MB * GB
    ) -> tuple[list[str], dict[str, float], int]:
        """
        Support method to get list input files and their profile
        :param path: input path
        :param files_to_use: file extensions to use
        :param max_file_size: max file size
        :param min_file_size: min file size
        :param cm_files: overwrite for the m_files in the class
        :return: tuple of file list, profile and number of retries
        """
        # Get files list.
        p_list = []
        total_input_file_size = 0
        i = 0
        files, retries = self.arrS3.list_files(path)
        for file in files:
            if i >= cm_files > 0:
                break
            # Only use specified files
            f_name = str(file["name"])
            name_extension = TransformUtils.get_file_extension(f_name)
            if name_extension[1] in files_to_use:
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
            retries,
        )

    def _get_input_files(
        self,
        input_path: str,
        output_path: str,
        cm_files: int,
        max_file_size: int = 0,
        min_file_size: int = MB * GB,
    ) -> tuple[list[str], dict[str, float], int]:
        """
        Get list and size of files from input path, that do not exist in the output path
        :param input_path: input path
        :param output_path: output path
        :param cm_files: max files to get
        :return: tuple of file list, profile and number of retries
        """
        if not self.checkpoint:
            return self._get_files_folder(
                path=input_path,
                files_to_use=self.files_to_use,
                cm_files=cm_files,
                min_file_size=min_file_size,
                max_file_size=max_file_size,
            )
        pout_list, _, retries1 = self._get_files_folder(
            path=output_path, files_to_use=self.files_to_checkpoint, cm_files=-1
        )
        output_base_names_ext = [file.replace(self.output_folder, self.input_folder) for file in pout_list]
        # In the case of binary transforms, an extension can be different, so just use the file names.
        # Also remove duplicates
        output_base_names = list(set([TransformUtils.get_file_extension(file)[0] for file in output_base_names_ext]))
        p_list = []
        total_input_file_size = 0
        i = 0
        files, retries = self.arrS3.list_files(input_path)
        retries += retries1
        for file in files:
            if i >= cm_files > 0:
                break
            # Only use .parquet files
            f_name = str(file["name"])
            name_extension = TransformUtils.get_file_extension(f_name)
            if name_extension[1] in self.files_to_use and name_extension[0] not in output_base_names:
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
            retries,
        )

    def get_files_to_process_internal(self) -> tuple[list[str], dict[str, float], int]:
        """
        Get files to process
        :return: list of files a dictionary of the files profile:
            "max_file_size",
            "min_file_size",
            "total_file_size"
        and retries
        """
        if self.output_folder is None:
            logger.error("Get files to process. S3 configuration is not present, returning empty")
            return [], {}, 0
        # Check if we are using data sets
        if self.d_sets is not None:
            # get folders for the input
            folders_to_use = []
            folders, retries = self.arrS3.list_folders(self.input_folder)
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
                    plist, profile, retries1 = self._get_input_files(
                        input_path=self.input_folder + folder,
                        output_path=self.output_folder + folder,
                        cm_files=cm_files,
                        min_file_size=min_file_size,
                        max_file_size=max_file_size,
                    )
                    retries += retries1
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
            path_list, profile, retries = self._get_input_files(
                input_path=self.input_folder,
                output_path=self.output_folder,
                cm_files=self.m_files,
            )
        return path_list, profile, retries

    def get_table(self, path: str) -> tuple[pyarrow.table, int]:
        """
        Get pyArrow table for a given path
        :param path - file path
        :return: pyArrow table or None, if the table read failed and number of retries
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

    def save_table(self, path: str, table: pyarrow.Table) -> tuple[int, dict[str, Any], int]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory, a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of retries
        """
        return self.arrS3.save_table(key=path, table=table)

    def save_job_metadata(self, metadata: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """
        Save metadata
        :param metadata: a dictionary, containing the following keys:
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
        in the case of failure dict is None and number of retries
        """
        if self.output_folder is None:
            logger.error("S3 configuration is not provided, can't save metadata")
            return None, 0
        metadata["source"] = {"name": self.input_folder, "type": "path"}
        metadata["target"] = {"name": self.output_folder, "type": "path"}
        return self.save_file(path=f"{self.output_folder}metadata.json", data=json.dumps(metadata, indent=2).encode())

    def get_file(self, path: str) -> tuple[bytes, int]:
        """
        Get file as a byte array
        :param path: file path
        :return: bytes array of file content and amount of retries
        """
        filedata, retries = self.arrS3.read_file(path)
        if path.endswith("gz"):
            filedata = gzip.decompress(filedata)
        return filedata, retries

    def get_folder_files(
        self, path: str, extensions: list[str] = None, return_data: bool = True
    ) -> tuple[dict[str, bytes], int]:
        """
        Get a list of byte content of files. The path here is an absolute path and can be anywhere.
        :param path: file path
        :param extensions: a list of file extensions to include. If None, then all files from this and
                           child ones will be returned
        :param return_data: flag specifying whether the actual content of files is returned (True), or just
                            directory is returned (False)
        :return: A dictionary of file names/binary content will be returned and number of retries
        """

        def _get_file_content(name: str, dt: bool) -> tuple[bytes, int]:
            """
            return file content
            :param name: file name
            :param dt: flag to return data or None
            :return: file content, number of retries
            """
            if dt:
                return self.get_file(name)
            return None, 0

        result = {}
        files, retries = self.arrS3.list_files(key=TransformUtils.clean_path(path))
        for file in files:
            f_name = str(file["name"])
            if extensions is None:
                if not f_name.endswith("/"):
                    # skip folders
                    b, retries1 = _get_file_content(f_name, return_data)
                    retries += retries1
                    result[f_name] = b
            else:
                for ext in extensions:
                    if f_name.endswith(ext):
                        # include the file
                        b, retries1 = _get_file_content(f_name, return_data)
                        retries += retries1
                        result[f_name] = b
                        break
        return result, retries

    def save_file(self, path: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Save byte array to the file
        :param path: file path
        :param data: byte array
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of retries
        """
        return self.arrS3.save_file(key=path, data=data)
