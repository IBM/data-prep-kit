import time
from typing import Any

import ray


@ray.remote(scheduling_strategy="SPREAD")
class TableProcessor:
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
            processor: local processor (mutator) class
            processor_params: dictionary of parameters for local processor
            stats: object reference to statistics
        """
        # Create data access
        self.data_access = params.get("data_access_factory", None).create_data_access()
        # Add data access ant statistics to the processor parameters
        processor_params = params.get("processor_params", None)
        processor_params["data_access"] = self.data_access
        processor_params["statistics"] = params.get("stats", None)
        # Create local processor
        self.processor = params.get("processor", None)(processor_params)
        # Create statistics
        self.stats = processor_params.get("statistics", None)

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
        self.stats.add_stats.remote({"source_documents": 1, "source_size": table.nbytes})
        # Process input table
        try:
            if table.num_rows > 0:
                # execute local processing
                out_tables = self.processor.transform(in_table=table)
                if len(out_tables) == 0:
                    self.stats.add_stats.remote({"table_processing": time.time() - t_start})
                    return
            else:
                print(f"table: {f_name} is empty, skipping processing")
                out_tables = [table]
            # Compute output file location. Preserve sub folders for Wisdom
            output_name = self.data_access.get_output_location(path=f_name)
            # save results
            if len(out_tables) == 0:
                # Nothing to write - return
                return
            if len(out_tables) == 1:
                # we have exactly 1 table
                output_file_size, save_res = self.data_access.save_table(path=output_name, table=out_tables[0])
                if save_res is not None:
                    # Store execution statistics. Doing this async
                    self.stats.add_stats.remote(
                        {
                            "result_documents": 1,
                            "result_size": out_tables[0].nbytes,
                            "table_processing": time.time() - t_start,
                        }
                    )
            else:
                # We have multiple tables
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
                        "result_documents": len(out_tables),
                        "result_size": table_sizes,
                        "table_processing": time.time() - t_start,
                    }
                )

        except Exception as e:
            print(f"Exception {e} processing file {f_name}")
