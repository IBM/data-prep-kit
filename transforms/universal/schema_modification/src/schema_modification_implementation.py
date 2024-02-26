import ray
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
)
from data_processing.data_access import DataAccess
from data_processing.transform import AbstractTableTransform
from data_processing.utils import TransformUtils


@ray.remote(num_cpus=0.25, scheduling_strategy="SPREAD")
class IDGenerator(object):
    """
    An actor maintaining unique integer ids
    """
    def __init__(self):
        """
        Initialization
        """
        self.id = 1

    def get_ids(self, n_rows: int) -> int:
        """
        Give out a new portion of integer ids
        :param n_rows: number of required Ids
        :return: starting value of blocks of ids
        """
        start_id = self.id
        self.id = self.id + n_rows
        return start_id


class SchemaTransform(AbstractTableTransform):
    """
    Implements schema modification of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        super().__init__(config)
        self.doc_column = config.get("doc_column", "")
        self.doc_id_column = config.get("doc_id_column", None)
        self.doc_id_int_column = config.get("doc_id_int_column", None)
        self.columns_to_remove = config.get("columns_to_remove", None)
        self.id_generator = config.get("id_generator", None)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        if self.doc_id_column is not None and TransformUtils.validata_columns(table=table, required=[self.doc_column]):
            # add doc id column
            docs = table[self.doc_column]
            doc_ids = [""] * table.num_rows
            for n in range(table.num_rows):
                doc_ids[n] = TransformUtils.str_to_hash(docs[n].as_py())
            table = TransformUtils.add_column(table=table, name=self.doc_id_column, content=doc_ids)
        if self.doc_id_int_column is not None:
            # add integer document id
            sid = ray.get(self.id_generator.get_ids.remote(table.num_rows))
            int_doc_ids = list(range(sid, table.num_rows + sid))
            table = TransformUtils.add_column(table=table, name=self.doc_id_int_column, content=int_doc_ids)
        if self.columns_to_remove is not None and len(self.columns_to_remove) > 0:
            try:
                table = table.drop(self.columns_to_remove)
            except Exception as e:
                print(f"Failed to remove columns {self.columns_to_remove} from table; error {e}")
        return [table], {}


class SchemaRuntime(DefaultTableTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            doc_column - name of the doc column
            doc_id_column - name of doc id column to create
            doc_id_int_column - name of integer doc id column to create
            columns_to_remove - list of columns to remove
        """
        super().__init__(params)

    def set_environment(self, data_access: DataAccess) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access - data access class
        :return: dictionary of filter init params
        """
        # create hashes

        return {"id_generator": IDGenerator.remote} | self.params


class SchemaTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="SchemaTransform", runtime_class=SchemaRuntime,
                         transform_class=SchemaTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument("--doc_column", type=str, default="contents", help="doc column name")
        parser.add_argument("--id_column", type=str, default=None, help="doc data id column name")
        parser.add_argument("--int_id_column", type=str, default=None, help="int doc data id column name")
        # When using this specify columns as follows --columns_to_remove foo bar foobar
        parser.add_argument("--columns_to_remove", type=str, default=None, nargs="+",
                            help="List of columns to remove")

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.id_column is not None and len(args.doc_column) <= 1:
            print("doc column name has to be defined for doc id generation")
            return False
        self.params["doc_column"] = args.doc_column
        self.params["doc_id_column"] = args.id_column
        self.params["doc_id_int_column"] = args.int_id_column
        self.params["columns_to_remove"] = args.columns_to_remove
        print(f"Schema modification parameters are : {self.params}")
        return True


