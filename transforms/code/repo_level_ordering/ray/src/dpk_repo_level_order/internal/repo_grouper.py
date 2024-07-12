import logging
import os
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
import ray
from data_processing.utils import get_logger
from data_processing_ray.runtime.ray import RayUtils
from pyarrow.fs import FileSelector, FileType, LocalFileSystem, S3FileSystem
from pyarrow.parquet import ParquetDataset, write_table


class DataAccessAlternative:
    def __init__(self, key=None, secret=None, endpoint=None):
        if key and secret:
            self.fs = S3FileSystem(
                access_key=key,
                secret_key=secret,
                endpoint_override=endpoint,
                request_timeout=20,
                connect_timeout=20,
            )
        else:
            self.fs = LocalFileSystem()

    def get_table(self, input_file_path):
        table = ParquetDataset(filesystem=self.fs, path_or_paths=input_file_path).read()
        return table, 0

    def save_table(self, output_file_path, out_table):
        out_folder = output_file_path.replace(os.path.basename(output_file_path), "")
        self.fs.create_dir(path=out_folder)
        write_table(table=out_table, where=output_file_path, filesystem=self.fs)
        return len(out_table), {}, 0


class GroupByRepo:
    """
    This class can read a list of parquet files and transform the rows of
    `repo_column_name` and apply the user provided mapper function to them
    and write the result to local disk or s3 bucket.

    """

    def __init__(
        self,
        repo_column_name,
        output_dir,
        logger,
        data_access,
        table_mapper=None,
    ):
        self.repo_column_name = repo_column_name
        self.output_dir = output_dir
        self.logger = get_logger(__name__)
        self.data_access = data_access
        self.enable_superrows = True
        self.table_mapper = table_mapper
        if self.table_mapper is None:
            """
            table_mapper is a function of signature: func(table: pa.Table, filename: str)-> List[Tuple[pa.Table, filename]]:
            """
            self.table_mapper = self._default_mapper_func

    def _default_mapper_func(self, table, file_name):
        return [
            (table, file_name),
        ]

    def process(self, repo: str, files: List[str]):
        cumulated_table = self._read_parquet_bulk(files)
        repo_table = self._filter_table_by_column(cumulated_table, self.repo_column_name, repo)

        def sanitize_path(repo_name):
            return repo_name.replace("/", "%2F")

        repo = sanitize_path(repo)
        tables = self.table_mapper(repo_table, repo)

        for out_table, filename in tables:

            self.logger.info(f"Write {filename}, tables: {len(out_table)}")
            self._write_parquet(out_table, filename)

    def _write_parquet(self, table, repo_name):
        # since we already know the repo
        # self.output_path should have the basepath where to write
        parquet_path = os.path.join(self.output_dir, f"{repo_name}.parquet")
        self.data_access.save_table(os.path.normpath(parquet_path), table)

    def _read_parquet_bulk(self, files):
        tables = list(
            map(
                lambda x: self.data_access.get_table(os.path.normpath(x))[0].to_pandas(),
                files,
            )
        )
        df = pd.concat(tables)
        return pa.Table.from_pandas(df)

    def _filter_table_by_column(self, table: pa.Table, column_name: str, column_value: str) -> pa.Table:
        """
        Filters rows in a PyArrow table based on the specified column value.

        Args:
            table (pa.Table): The input PyArrow table.
            column_name (str): The name of the column to filter.
            column_value (str): The value to match in the specified column.

        Returns:
            pa.Table: A new table containing only rows where the specified column has the given value.
        """

        column_data = table.column(column_name)
        row_mask = pa.compute.equal(column_data, column_value)
        filtered_table = table.filter(row_mask)

        return filtered_table


@ray.remote(scheduling_strategy="SPREAD")
class GroupByRepoActor(GroupByRepo):
    """
    This Actor represents a proxy to the class `GroupByRepo`.

    A sample `params` dict for this actor looks like.

      params = {
         'repo_column_name': str,
         'output_dir': str,
         'mapper': A function with signature: func(table: pa.Table, filename: str)->List[Tuple[pa.Table, str]]
         'data_access_creds': A dict os s3 creds or None eg. {'access_key': <>, 'secret_key': <>, 'url': <>}
      }

    """

    def __init__(self, params: dict):
        if params["data_access_creds"] is not None:
            access_key = params["data_access_creds"]["access_key"]
            secret_key = params["data_access_creds"]["secret_key"]
            url = params["data_access_creds"]["url"]
        else:
            access_key, secret_key, url = (None, None, None)
        super().__init__(
            params["repo_column_name"],
            params["output_dir"],
            None,  # params["logger"],
            params["data_access_factory"].create_data_access(),  # DataAccessAlternative(access_key, secret_key, url),
            params["mapper"],
        )
