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

import os
from typing import List

import pandas as pd
import pyarrow as pa
import ray
from data_processing.utils import get_logger


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
        try:
            repo_table = self._read_table_for_group(self.repo_column_name, repo, files)
            if len(repo_table) == 0:
                # not processing empty table
                return

            def sanitize_path(repo_name):
                return repo_name.replace("/", "%2F")

            repo = sanitize_path(repo)
            tables = self.table_mapper(repo_table, repo)

            for out_table, filename in tables:

                self.logger.info(f"Write {filename}, tables: {len(out_table)}")
                self._write_parquet(out_table, filename)
        except Exception as e:
            self.logger.error(f"Failed processing repo: {repo}. {e}")

    def _write_parquet(self, table, repo_name):
        # since we already know the repo
        # self.output_path should have the basepath where to write
        parquet_path = os.path.join(self.output_dir, f"{repo_name}.parquet")
        self.data_access.save_table(os.path.normpath(parquet_path), table)

    def _read_table_for_group(self, grouping_column, group, files):
        """This function reads the files and filters the tables based on grouping_column value"""
        dfs = []
        for file in files:
            table, _ = self.data_access.get_table(os.path.normpath(file))
            # filtering each table is more memory efficient than
            # reading all tables and filtering later.
            filtered_table = self._filter_table_by_column(table, grouping_column, group)
            dfs.append(filtered_table.to_pandas())
        df = pd.concat(dfs)

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
        super().__init__(
            params["repo_column_name"],
            params["output_dir"],
            None,
            params["data_access_factory"].create_data_access(),
            params["mapper"],
        )
