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

from data_processing.utils import TransformUtils, get_logger


class AbstractTransformFileProcessor:
    """
    This is the the base class implementing processing of a single binary file
    """

    def __init__(self):
        """
        Init method
        """
        self.data_access = None
        self.transform = None
        self.stats = None
        self.last_file_name = None
        self.last_extension = None
        self.last_file_name_next_index = None
        self.logger = get_logger(__name__)

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
        # Read source file
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
            # execute local processing
            name_extension = TransformUtils.get_file_extension(f_name)
            self.logger.debug(f"Begin transforming file {f_name}")
            out_files, stats = self.transform.transform_binary(file_name=f_name, byte_array=filedata)
            self.logger.debug(f"Done transforming file {f_name}, got {len(out_files)} files")
            self.last_file_name = name_extension[0]
            self.last_file_name_next_index = None
            self.last_extension = name_extension[1]
            # save results
            self._submit_file(t_start=t_start, out_files=out_files, stats=stats)
        except Exception as e:
            self.logger.warning(f"Exception {e} processing file {f_name}: {traceback.format_exc()}")
            self._publish_stats({"transform execution exception": 1})

    def flush(self) -> None:
        """
        This is supporting method for transformers, that implement buffering of data, for example resize.
        These transformers can have buffers containing data that were not written to the output. Flush is
        the hook for them to return back locally stored data and their statistics.
        :return: None
        """
        if self.last_file_name is None:
            # for some reason a given worker never processed anything. Happens in testing
            # when the amount of workers is greater then the amount of files
            self.logger.debug("skipping flush, no name for file is defined")
            return
        try:
            t_start = time.time()
            # get flush results
            self.logger.debug(
                f"Begin flushing transform, last file name {self.last_file_name}, last index {self.last_file_name_next_index}"
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
            case 1:
                # we have exactly 1 output file
                file_ext = out_files[0]
                lfn = self.last_file_name
                if self.last_file_name_next_index is not None:
                    lfn = f"{lfn}_{self.last_file_name_next_index}"
                output_name = self.data_access.get_output_location(path=f"{lfn}{file_ext[1]}")
                self.logger.debug(
                    f"Writing transformed file {self.last_file_name}{self.last_extension} to {output_name}"
                )
                save_res, retries = self.data_access.save_file(path=output_name, data=file_ext[0])
                if retries > 0:
                    self._publish_stats({"data access retries": retries})
                if save_res is not None:
                    # Store execution statistics. Doing this async
                    self._publish_stats(
                        {
                            "result_files": 1,
                            "result_size": len(file_ext[0]),
                            "processing_time": time.time() - t_start,
                        }
                    )
                else:
                    self.logger.warning(f"Failed to write file {output_name}")
                    self._publish_stats({"failed_writes": 1})
                if self.last_file_name_next_index is None:
                    self.last_file_name_next_index = 0
                else:
                    self.last_file_name_next_index += 1
            case _:
                # we have more then 1 file
                file_sizes = 0
                output_file_name = self.data_access.get_output_location(path=self.last_file_name)
                start_index = self.last_file_name_next_index
                if start_index is None:
                    start_index = 0
                count = len(out_files)
                for index in range(count):
                    file_ext = out_files[index]
                    output_name_indexed = f"{output_file_name}_{start_index + index}{file_ext[1]}"
                    file_sizes += len(file_ext[0])
                    self.logger.debug(
                        f"Writing transformed file {self.last_file_name}{self.last_extension}, {index + 1} "
                        f"of {count}  to {output_name_indexed}"
                    )
                    save_res, retries = self.data_access.save_file(path=output_name_indexed, data=file_ext[0])
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
