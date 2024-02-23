import time
from typing import Any
import pyarrow as pa

import ray


@ray.remote(scheduling_strategy="SPREAD")
class TransformTableProcessor:
    """
    This is the class implementing the actual work/actor processing of a single pyarrow file
    """

    def __init__(
        self,
        params: dict[str, Any],
    ):
        """
        Init method
        :param params: dictionary that has the following key
            data_access_factory: data access factory
            transform_class: local transform class
            transform_params: dictionary of parameters for local transform creation
            stats: object reference to statistics
        """
        # Create data access
        self.data_access = params.get("data_access_factory", None).create_data_access()
        # Add data access ant statistics to the processor parameters
        transform_params = params.get("transform_params", None)
        transform_params["data_access"] = self.data_access
        transform_params["statistics"] = params.get("stats", None)
        # Create local processor
        self.transform = params.get("transform_class", None)(transform_params)
        # Create statistics
        self.stats = transform_params.get("statistics", None)
        self.last_empty = ""

    def process_table(self, f_name: str) -> None:
        """
        Method processing an individual file
        :param f_name: file name
        :return: None
        """
        if self.data_access is None:
            return
        t_start = time.time()
        # Read source table
        table = self.data_access.get_table(path=f_name)
        if table is None:
            return
        self.stats.add_stats.remote({"source_files": 1, "source_size": table.nbytes})
        # Process input table
        try:
            if table.num_rows > 0:
                # execute local processing
                out_tables, stats = self.transform.transform(table=table)
            else:
                print(f"table: {f_name} is empty, skipping processing")
                out_tables = [table]
                stats = {}
            # save results
            self._submit_table(f_name=f_name, t_start=t_start, out_tables=out_tables, stats=stats)
        except Exception as e:
            print(f"Exception {e} processing file {f_name}")

    def flush(self) -> None:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics.
        :return: None
        """
        t_start = time.time()
        try:
            # get flush results
            out_tables, stats = self.transform.flush()
            # Here we are using the name of the last table, that did not return anything
            self._submit_table(f_name=self.last_empty, t_start=t_start, out_tables=out_tables, stats=stats)
        except Exception as e:
            print(f"Exception {e} flushing")

    def _submit_table(self, f_name: str, t_start: float, out_tables: list[pa.Table], stats: dict[str, Any]) \
            -> None:
        """
        This is a helper method writing output tables and statistics
        :param f_name: input file n
        :param t_start: execution start time
        :param out_tables: list of tables to write
        :param stats: execution statistics to populate
        :return: None
        """
        # Compute output file location. Preserve sub folders for Wisdom
        output_name = self.data_access.get_output_location(path=f_name)
        match len(out_tables):
            case 0:
                # no tables - save input file name for flushing
                self.last_empty = f_name
            case 1:
                # we have exactly 1 table
                output_file_size, save_res = self.data_access.save_table(path=output_name, table=out_tables[0])
                if save_res is not None:
                    # Store execution statistics. Doing this async
                    self.stats.add_stats.remote(
                        {
                            "result_files": 1,
                            "result_size": out_tables[0].nbytes,
                            "table_processing": time.time() - t_start,
                        }
                    )
            case _:
                # we have more then 1 table
                table_sizes = 0
                output_file_name = output_name.removesuffix(".parquet")
                for index in range(len(out_tables)):
                    table_sizes += out_tables[index].nbytes
                    output_file_size, save_res = self.data_access.save_table(
                        path=f"{output_file_name}_{index}.parquet", table=out_tables[index]
                    )
                    if save_res is None:
                        break
                self.stats.add_stats.remote(
                    {
                        "result_files": len(out_tables),
                        "result_size": table_sizes,
                        "table_processing": time.time() - t_start,
                    }
                )
        # save statistics
        if len(stats) > 0:
            self.stats.add_stats.remote(stats)
