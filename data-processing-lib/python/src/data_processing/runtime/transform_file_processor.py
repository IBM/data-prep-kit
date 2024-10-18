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
import time
import traceback
from typing import Any

from data_processing.data_access import DataAccessFactoryBase
from data_processing.utils import TransformUtils, UnrecoverableException, get_logger


class AbstractTransformFileProcessor:
    """
    This is the the base class implementing processing of a single binary file
    """

    def __init__(
        self,
        data_access_factory: DataAccessFactoryBase,
        transform_parameters: dict[str, Any],
        is_folder: bool = False,
    ):
        """
        Init method
        :param data_access_factory: Data Access Factory
        :param transform_parameters: Transform parameters
        :param is_folder: folder transform flag
        """
        self.logger = get_logger(__name__)
        # validate parameters
        if data_access_factory is None:
            self.logger.error("Transform file processor: data access factory is not specified")
            raise UnrecoverableException("data access factory is None")
        self.transform = None
        self.stats = None
        self.last_file_name = None
        self.last_extension = None
        self.last_file_name_next_index = None
        self.data_access = data_access_factory.create_data_access()
        # Add data access and statistics to the processor parameters
        self.transform_params = transform_parameters
        self.transform_params["data_access"] = self.data_access
        self.is_folder = is_folder

    def process_file(self, f_name: str) -> None:
        """
        Method processing an individual file
        :param f_name: file name
        :return: None
        """
        self.logger.debug(f"Begin processing file {f_name}")
        if self.data_access is None:
            self.logger.warning("No data_access found. Returning.")
            return
        t_start = time.time()
        if not self.is_folder:
            # Read source file only if we are processing file
            filedata, retries = self.data_access.get_file(path=f_name)
            if retries > 0:
                self._publish_stats({"data access retries": retries})
            if filedata is None:
                self.logger.warning(f"File read resulted in None for {f_name}. Returning.")
                self._publish_stats({"failed_reads": 1})
                return
            self._publish_stats({"source_files": 1, "source_size": len(filedata)})
        # Process input file
        try:
            self.logger.debug(f"Begin transforming file {f_name}")
            if not self.is_folder:
                # execute local processing
                out_files, stats = self.transform.transform_binary(file_name=f_name, byte_array=filedata)
                name_extension = TransformUtils.get_file_extension(f_name)
                self.last_file_name = name_extension[0]
                self.last_file_name_next_index = None
                self.last_extension = name_extension[1]
            else:
                out_files, stats = self.transform.transform(folder_name=f_name)
                self.last_file_name = f_name
            self.logger.debug(f"Done transforming file {f_name}, got {len(out_files)} files")
            # save results
            self._submit_file(t_start=t_start, out_files=out_files, stats=stats)
        # Process unrecoverable exceptions
        except UnrecoverableException as _:
            self.logger.warning(f"Transform has thrown unrecoverable exception processing file {f_name}. Exiting...")
            raise UnrecoverableException
        # Process other exceptions
        except Exception as e:
            self.logger.warning(f"Exception processing file {f_name}: {traceback.format_exc()}")
            self._publish_stats({"transform execution exception": 1})

    def flush(self) -> None:
        """
        This is supporting method for transformers, that implement buffering of data, for example resize.
        These transformers can have buffers containing data that were not written to the output. Flush is
        the hook for them to return back locally stored data and their statistics.
        :return: None
        """
        if self.last_file_name is None or self.is_folder:
            # for some reason a given worker never processed anything. Happens in testing
            # when the amount of workers is greater than the amount of files
            self.logger.debug("skipping flush, no name for file is defined or this is a folder transform")
            return
        try:
            t_start = time.time()
            # get flush results
            self.logger.debug(
                f"Begin flushing transform, last file name {self.last_file_name}, "
                f"last index {self.last_file_name_next_index}"
            )
            out_files, stats = self.transform.flush_binary()
            self.logger.debug(f"Done flushing transform, got {len(out_files)} files")
            # Here we are using the name of the last file, that we were processing
            self._submit_file(t_start=t_start, out_files=out_files, stats=stats)
        except Exception as e:
            self.logger.warning(f"Exception {e} flushing: {traceback.format_exc()}")
            self._publish_stats({"transform execution exception": 1})

    def _submit_file(self, t_start: float, out_files: list[tuple[bytes, str]], stats: dict[str, Any]) -> None:
        """
        This is a helper method writing output files and statistics
        :param t_start: execution start time
        :param out_files: list of files to write
        :param stats: execution statistics to populate
        :return: None
        """
        self.logger.debug(
            f"submitting files under file named {self.last_file_name}{self.last_extension} "
            f"number of files {len(out_files)}"
        )
        match len(out_files):
            case 0:
                # no output file - save input file name for flushing
                self.logger.debug(
                    f"Transform did not produce a transformed file for " f"file {self.last_file_name}.parquet"
                )
                self._publish_stats(
                    {
                        "result_files": len(out_files),
                        "processing_time": time.time() - t_start,
                    }
                )
            case 1:
                # we have exactly 1 output file
                if self.is_folder:
                    # its folder
                    output_name = out_files[0][1]
                    dt = out_files[0][0]
                else:
                    file_ext = out_files[0]
                    lfn = self.last_file_name
                    if self.last_file_name_next_index is not None:
                        lfn = f"{lfn}_{self.last_file_name_next_index}"
                    output_name = self.data_access.get_output_location(path=f"{lfn}{file_ext[1]}")
                    dt = file_ext[0]
                self.logger.debug(
                    f"Writing transformed file {self.last_file_name}{self.last_extension} to {output_name}"
                )
                save_res, retries = self.data_access.save_file(path=output_name, data=dt)
                if retries > 0:
                    self._publish_stats({"data access retries": retries})
                if save_res is None:
                    self.logger.warning(f"Failed to write file {output_name}")
                    self._publish_stats({"failed_writes": 1})
                # Store execution statistics. Doing this async
                self._publish_stats(
                    {
                        "result_files": 1,
                        "result_size": len(dt),
                        "processing_time": time.time() - t_start,
                    }
                )
                if self.last_file_name_next_index is None:
                    self.last_file_name_next_index = 0
                else:
                    self.last_file_name_next_index += 1
            case _:
                # we have more than 1 file
                file_sizes = 0
                output_file_name = self.data_access.get_output_location(path=self.last_file_name)
                start_index = self.last_file_name_next_index
                if start_index is None:
                    start_index = 0
                count = len(out_files)
                for index in range(count):
                    if self.is_folder:
                        # its a folder
                        output_name_indexed = out_files[index][1]
                        dt = out_files[index][0]
                    else:
                        # files
                        file_ext = out_files[index]
                        output_name_indexed = f"{output_file_name}_{start_index + index}{file_ext[1]}"
                        self.logger.debug(
                            f"Writing transformed file {self.last_file_name}{self.last_extension}, {index + 1} "
                            f"of {count}  to {output_name_indexed}"
                        )
                        dt = file_ext[0]
                    file_sizes += len(dt)
                    save_res, retries = self.data_access.save_file(path=output_name_indexed, data=dt)
                    if retries > 0:
                        self._publish_stats({"data access retries": retries})
                    if save_res is None:
                        self.logger.warning(f"Failed to write file {output_name_indexed}")
                        self._publish_stats({"failed_writes": 1})
                        break
                self.last_file_name_next_index = start_index + count
                self._publish_stats(
                    {
                        "result_files": len(out_files),
                        "result_size": file_sizes,
                        "processing_time": time.time() - t_start,
                    }
                )
        # save transformer's statistics
        if len(stats) > 0:
            self._publish_stats(stats)

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publishing execution statistics
        :param stats: Statistics
        :return: None
        """
        raise ValueError("must be implemented by subclass")
