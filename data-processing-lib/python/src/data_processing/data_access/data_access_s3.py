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
from data_processing.utils import TransformUtils


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
        super().__init__(d_sets=d_sets, checkpoint=checkpoint, m_files=m_files, n_samples=n_samples,
                         files_to_use=files_to_use, files_to_checkpoint=files_to_checkpoint)
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
        self.arrS3 = ArrowS3(
            access_key=s3_credentials.get("access_key"),
            secret_key=s3_credentials.get("secret_key"),
            endpoint=s3_credentials.get("url", None),
            region=s3_credentials.get("region", None),
        )

    def get_output_folder(self) -> str:
        """
        Get output folder as a string
        :return: output_folder
        """
        return self.output_folder

    def get_input_folder(self) -> str:
        """
        Get input folder as a string
        :return: input_folder
        """
        return self.input_folder

    def _list_files_folder(self, path: str) -> tuple[list[dict[str, Any]], int]:
        """
        Get files for a given folder and all sub folders
        :param path: path
        :return: List of files
        """
        try:
            return self.arrS3.list_files(key=path)
        except Exception as e:
            self.logger.error(f"Error listing S3 files for path {path} - {e}")
            return [], 0

    def _get_folders_to_use(self) -> tuple[list[str], int]:
        """
        convert data sets to a list of folders to use
        :return: list of folders and retries
        """
        folders_to_use = []
        try:
            folders, retries = self.arrS3.list_folders(self.input_folder)
        except Exception as e:
            self.logger.error(f"Error listing S3 folders for path {self.input_folder} - {e}")
            return [], 0
        # Only use valid folders
        for folder in folders:
            s_folder = folder[:-1]
            for s_name in self.d_sets:
                if s_folder.endswith(s_name):
                    folders_to_use.append(folder)
                    break
        return folders_to_use, retries

    def get_table(self, path: str) -> tuple[pyarrow.table, int]:
        """
        Get pyArrow table for a given path
        :param path - file path
        :return: pyArrow table or None, if the table read failed and number of retries
        """
        try:
            return self.arrS3.read_table(path)
        except Exception as e:
            self.logger.error(f"Exception reading table {path} from S3 - {e}")
            return None, 0

    def save_table(self, path: str, table: pyarrow.Table) -> tuple[int, dict[str, Any], int]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory, a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of retries
        """
        try:
            return self.arrS3.save_table(key=path, table=table)
        except Exception as e:
            self.logger.error(f"Exception saving table to S3 {path} - {e}")
            return 0, {}, 0

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
            self.logger.error("S3 configuration is not provided, can't save metadata")
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
        try:
            filedata, retries = self.arrS3.read_file(path)
        except Exception as e:
            self.logger.error(f"Exception reading file {path} - {e}")
            return None, 0
        if path.endswith("gz"):
            filedata = gzip.decompress(filedata)
        return filedata, retries

    def save_file(self, path: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Save byte array to the file
        :param path: file path
        :param data: byte array
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of retries
        """
        try:
            return self.arrS3.save_file(key=path, data=data)
        except Exception as e:
            self.logger.error(f"Exception saving file {path} - {e}")