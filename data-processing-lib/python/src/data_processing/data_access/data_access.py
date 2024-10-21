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

import random
from typing import Any

import pyarrow as pa
from data_processing.utils import KB, MB, GB, TransformUtils, get_logger


class DataAccess:
    """
    Base class for data access (interface), defining all the methods
    """
    def __init__(
            self,
            d_sets: list[str],
            checkpoint: bool,
            m_files: int,
            n_samples: int,
            files_to_use: list[str],
            files_to_checkpoint: list[str],
    ):
        """
        Create data access class for folder based configuration
        :param d_sets list of the data sets to use
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return
        :param n_samples: amount of files to randomly sample
        :param files_to_use: files extensions of files to include
        :param files_to_checkpoint: files extensions of files to use for checkpointing
        """
        self.d_sets = d_sets
        self.checkpoint = checkpoint
        self.m_files = m_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use
        self.files_to_checkpoint = files_to_checkpoint
        self.logger = get_logger(__name__)

    def get_output_folder(self) -> str:
        """
        Get output folder as a string
        :return: output_folder
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_input_folder(self) -> str:
        """
        Get input folder as a string
        :return: input_folder
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_random_file_set(self, n_samples: int, files: list[str]) -> list[str]:
        """
        Get random set of files
        :param n_samples: set size
        :param files: list of original files
        :return: set of randomly selected files
        """
        # Pick files to include
        if len(files) > n_samples:
            # Pick files at random
            files_set = [int(random.random() * len(files)) for _ in range(n_samples)]
        else:
            # use all existing files
            files_set = range(len(files))
        result = [""] * len(files_set)
        index = 0
        for f in files_set:
            result[index] = files[f]
            index += 1
        self.logger.info(f"Using files {result} to sample data")
        return result

    def get_files_to_process(self) -> tuple[list[str], dict[str, float], int]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
        and the number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        if self.get_output_folder() is None:
            self.logger.warning("Input/Output are not defined, returning empty list")
            return [], {}, 0
        path_list, path_profile, retries = self._get_files_to_process_internal()
        if self.n_samples > 0:
            files = self.get_random_file_set(n_samples=self.n_samples, files=path_list)
            return files, path_profile, retries
        return path_list, path_profile, retries

    def _get_files_to_process_internal(self) -> tuple[list[str], dict[str, float], int]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
        and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        # Check if we are using data sets
        if self.d_sets is not None:
            # get folders for the input
            folders_to_use, retries = self._get_folders_to_use()
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
                        input_path=folder,
                        output_path=self.get_output_location(folder),
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
                input_path=self.get_input_folder(),
                output_path=self.get_output_folder(),
                cm_files=self.m_files,
            )
        return path_list, profile, retries

    def _get_folders_to_use(self) -> tuple[list[str], int]:
        """
        convert data sets to a list of folders to use
        :return: list of folders and retries
        """
        raise NotImplementedError("Subclasses should implement this!")

    def _get_files_folder(
            self,
            path: str,
            files_to_use: list[str],
            cm_files: int,
            max_file_size: int = 0,
            min_file_size: int = MB * GB
    ) -> tuple[list[dict[str, Any]], dict[str, float], int]:
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
        files, retries = self._list_files_folder(path=path)
        for file in files:
            if i >= cm_files > 0:
                break
            # Only use specified files
            f_name = str(file["name"])
            if files_to_use is not None:
                name_extension = TransformUtils.get_file_extension(f_name)
                if name_extension[1] not in files_to_use:
                    continue
            p_list.append(file)
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
            file_sizes, profile, retries = self._get_files_folder(
                path=input_path,
                files_to_use=self.files_to_use,
                cm_files=cm_files,
                min_file_size=min_file_size,
                max_file_size=max_file_size,
            )
            files = [fs["name"] for fs in file_sizes]
            return files, profile, retries

        pout_list, _, retries1 = self._get_files_folder(
            path=output_path, files_to_use=self.files_to_checkpoint, cm_files=-1
        )
        output_base_names_ext = [file["name"].replace(self.get_output_folder(), self.get_input_folder())
                                 for file in pout_list]
        # In the case of binary transforms, an extension can be different, so just use the file names.
        # Also remove duplicates
        output_base_names = list(set([TransformUtils.get_file_extension(file)[0] for file in output_base_names_ext]))
        p_list = []
        total_input_file_size = 0
        i = 0
        files, _, retries = self._get_files_folder(
            path=input_path, files_to_use=self.files_to_use, cm_files=-1
        )
        retries += retries1
        for file in files:
            if i >= cm_files > 0:
                break
            f_name = file["name"]
            name_extension = TransformUtils.get_file_extension(f_name)
            if self.files_to_use is not None:
                if name_extension[1] not in self.files_to_use:
                    continue
            if name_extension[0] not in output_base_names:
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

    def _list_files_folder(self, path: str) -> tuple[list[dict[str, Any]], int]:
        """
        Get files for a given folder and all sub folders
        :param path: path
        :return: List of files
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_table(self, path: str) -> tuple[pa.table, int]:
        """
        Get pyArrow table for a given path
        :param path - file path
        :return: pyArrow table or None, if the table read failed and number of operation retries.
                 Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_file(self, path: str) -> tuple[bytes, int]:
        """
        Get file as a byte array
        :param path: file path
        :return: bytes array of file content and number of operation retries
                 Retries are performed on operation failures and are typically due to the resource overload.

        """
        raise NotImplementedError("Subclasses should implement this!")

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
        :return: A dictionary of file names/binary content will be returned
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
        files, _, retries = self._get_files_folder(
            path=path, files_to_use=extensions, cm_files=-1
        )
        for file in files:
            f_name = str(file["name"])
            b, retries1 = _get_file_content(f_name, return_data)
            retries += retries1
            result[f_name] = b
        return result, retries

    def save_file(self, path: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Save byte array to the file
        :param path: file path
        :param data: byte array
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_output_location(self, path: str) -> str:
        """
        Get output location based on input
        :param path: input file location
        :return: output file location
        """
        if self.get_output_folder() is None:
            self.logger.error("Get out put location. S3 configuration is not provided, returning None")
            return None
        return path.replace(self.get_input_folder(), self.get_output_folder())

    def save_table(self, path: str, table: pa.Table) -> tuple[int, dict[str, Any], int]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def save_job_metadata(self, metadata: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """
        Save job metadata
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
        in the case of failure dict is None and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def sample_input_data(self, n_samples: int = 10) -> tuple[dict[str, Any], int]:
        """
        Sample input data set to get average table size, average doc size, number of docs, etc.
        Note that here we are not reading all of the input documents, but rather randomly pick
        their subset. It gives more precise answer as subset grows, but it takes longer
        :param n_samples: number of samples to use - default 10
        :return: a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
            average table size MB,
            average doc size KB,
            estimated number of docs
        and number of operation retries
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        # get files to process
        path_list, path_profile, retries = self._get_files_to_process_internal()
        # Pick files to sample
        files = self.get_random_file_set(n_samples=n_samples, files=path_list)
        # Read table and compute number of docs and sizes
        number_of_docs = []
        table_sizes = []
        n_tables = 0
        for f in files:
            table, r = self.get_table(path=f)
            retries += r
            if table is not None:
                n_tables += 1
                number_of_docs.append(table.num_rows)
                # As a table size is mostly document, we can consider them roughly the same
                table_sizes.append(table.nbytes)
        # compute averages
        if n_tables == 0:
            av_number_docs = 0
            av_table_size = 0
            av_doc_size = 0
        else:
            av_number_docs = sum(number_of_docs) / n_tables
            av_table_size = sum(table_sizes) / n_tables / MB
            if av_number_docs == 0:
                av_doc_size = 0
            else:
                av_doc_size = av_table_size * MB / av_number_docs / KB
        self.logger.info(
            f"average number of docs {av_number_docs}, average table size {av_table_size} MB, "
            f"average doc size {av_doc_size} kB"
        )

        # compute number of docs
        number_of_docs = av_number_docs * len(path_list)
        self.logger.info(f"Estimated number of docs {number_of_docs}")
        return (
            path_profile
            | {
                "average table size MB": av_table_size,
                "average doc size KB": av_doc_size,
                "estimated number of docs": number_of_docs,
            },
            retries,
        )
