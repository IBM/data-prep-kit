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

import pyarrow as pa
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics, TransformConfiguration
from data_processing.utils import TransformUtils, get_logger


logger = get_logger(__name__)


class TransformTableProcessor:
    """
    This is the class implementing the worker class processing of a single pyarrow file
    """

    def __init__(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: TransformStatistics,
        params: TransformConfiguration,
    ):
        """
        Init method
        :param data_access_factory - data access factory
        :param statistics - reference to statistics class
        :param params: transform configuration class
        """
        # Create data access
        self.data_access = data_access_factory.create_data_access()
        # Add data access and statistics to the processor parameters
        transform_params = dict(params.get_transform_params())
        transform_params["data_access"] = self.data_access
        transform_params["statistics"] = statistics
        # Create local processor
        self.transform = params.get_transform_class()(transform_params)
        # Create statistics
        self.stats = statistics
        self.last_file_name = None
        self.last_file_name_next_index = None

    def process_data(self, f_name: str) -> None:
        """
        Method processing an individual file
        :param f_name: file name
        :return: None
        """
        logger.debug(f"Begin processing file {f_name}")
        if self.data_access is None:
            logger.warning("No data_access found. Returning.")
            return
        t_start = time.time()
        # Read source table
        table = self.data_access.get_table(path=f_name)
        if table is None:
            logger.warning("File read resulted in None. Returning.")
            self.stats.add_stats({"failed_reads": 1})
            return
        self.stats.add_stats({"source_files": 1, "source_size": table.nbytes})
        # Process input table
        try:
            if table.num_rows > 0:
                # execute local processing
                logger.debug(f"Begin transforming table from {f_name}")
                out_tables, stats = self.transform.transform(table=table)
                logger.debug(f"Done transforming table from {f_name}")
                self.last_file_name = TransformUtils.get_file_extension(f_name)[0]
                self.last_file_name_next_index = None
            else:
                logger.info(f"table: {f_name} is empty, skipping processing")
                self.stats.add_stats({"skipped empty tables": 1})
                return
            # save results
            self._submit_table(t_start=t_start, out_tables=out_tables, stats=stats)
        except Exception as e:
            logger.warning(f"Exception {e} processing file {f_name}: {traceback.format_exc()}")
            self.stats.add_stats({"transform execution exception": 1})

    def flush(self) -> None:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics.
        :return: None
        """
        t_start = time.time()
        if self.last_file_name is None:
            # for some reason a given worker never processed anything. Happens in testing
            # when the amount of workers is greater then the amount of files
            logger.debug("skipping flush, no name for file is defined")
            return
        try:
            # get flush results
            logger.debug(f"Begin flushing transform")
            out_tables, stats = self.transform.flush()
            logger.debug(f"Done flushing transform, got {len(out_tables)} tables")
            # Here we are using the name of the last table, that we were processing
            self._submit_table(t_start=t_start, out_tables=out_tables, stats=stats)
        except Exception as e:
            logger.warning(f"Exception {e} flushing: {traceback.format_exc()}")
            self.stats.add_stats({"transform execution exception": 1})

    def _submit_table(self, t_start: float, out_tables: list[pa.Table], stats: dict[str, Any]) -> None:
        """
        This is a helper method writing output tables and statistics
        :param t_start: execution start time
        :param out_tables: list of tables to write
        :param stats: execution statistics to populate
        :return: None
        """
        logger.debug(f"submitting tables under file named {self.last_file_name}.parquet, "
                     f"number of tables {len(out_tables)}")
        # Compute output file location. Preserve sub folders for Wisdom
        match len(out_tables):
            case 0:
                # no tables - save input file name for flushing
                logger.debug(f"Transform did not produce a transformed table for file {self.last_file_name}.parquet")
            case 1:
                # we have exactly 1 table
                output_name = self.data_access.get_output_location(path=f"{self.last_file_name}.parquet")
                logger.debug(f"Writing transformed file {self.last_file_name}.parquet to {output_name}")
                if TransformUtils.verify_no_duplicate_columns(table=out_tables[0], file=output_name):
                    output_file_size, save_res = self.data_access.save_table(path=output_name, table=out_tables[0])
                    if save_res is not None:
                        # Store execution statistics. Doing this async
                        self.stats.add_stats(
                            {
                                "result_files": 1,
                                "result_size": out_tables[0].nbytes,
                                "table_processing": time.time() - t_start,
                            }
                        )
                    else:
                        logger.warning(f"Failed to write file {output_name}")
                        self.stats.add_stats({"failed_writes": 1})
                self.last_file_name_next_index = 0
            case _:
                # we have more then 1 table
                table_sizes = 0
                output_file_name = self.data_access.get_output_location(path=self.last_file_name)
                start_index = self.last_file_name_next_index
                if start_index is None:
                    start_index = 0
                count = len(out_tables)
                for index in range(count):
                    output_name_indexed = f"{output_file_name}_{start_index + index}.parquet"
                    if TransformUtils.verify_no_duplicate_columns(table=out_tables[index], file=output_name_indexed):
                        table_sizes += out_tables[index].nbytes
                        logger.debug(
                            f"Writing transformed file {self.last_file_name}.parquet, {index + 1} "
                            f"of {count}  to {output_name_indexed}"
                        )
                        output_file_size, save_res = self.data_access.save_table(
                            path=output_name_indexed, table=out_tables[index]
                        )
                        if save_res is None:
                            logger.warning(f"Failed to write file {output_name_indexed}")
                            self.stats.add_stats({"failed_writes": 1})
                            break
                self.last_file_name_next_index = start_index + count
                self.stats.add_stats(
                    {
                        "result_files": len(out_tables),
                        "result_size": table_sizes,
                        "table_processing": time.time() - t_start,
                    }
                )
        # save transformer's statistics
        if len(stats) > 0:
            self.stats.add_stats(stats)
