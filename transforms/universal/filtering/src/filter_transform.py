import argparse
import ast
from typing import Any

import duckdb
import pyarrow as pa
from data_processing.ray import DefaultTableTransformConfiguration, TransformLauncher
from data_processing.transform import AbstractTableTransform
from data_processing.utils import CLIArgumentProvider, ParamsUtils, get_logger


logger = get_logger(__name__)


short_name = "filter"
cli_prefix = short_name + "_"

sql_statement_key = "sql_statement"
""" Key holds the SQL statement used for filtering"""
sql_params_dict_key = "sql_params_dict"
""" AST Key holds the dictionary of SQL statement parameters"""

sql_statement_cli_param = f"{cli_prefix}{sql_statement_key}"
""" Key holds the SQL statement used for filtering"""
sql_params_dict_cli_param = f"{cli_prefix}{sql_params_dict_key}"
""" AST Key holds the dictionary of SQL statement parameters"""

captured_arg_keys = [sql_statement_key, sql_params_dict_key]
""" The set of keys captured from the command line """

# defaults
sql_statement_default = "SELECT * FROM table"
""" The default SQL statement used for filtering """
sql_params_dict_default = ast.literal_eval("{}")
""" The default dictionary of SQL statement parameters"""


class FilterTransform(AbstractTableTransform):
    """
    Implements filtering - select from a pyarrow.Table a set of rows that
    satisfy a set of filtering criteria
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, FilterTransformRuntime.  If running from the Ray orchestrator,
        these will be provided by that class with help from the RayMutatingDriver.
        """

        super().__init__(config)
        self.sql_statement = config.get(sql_statement_key, sql_statement_default)
        self.sql_params_dict = config.get(sql_params_dict_key, sql_params_dict_default)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:
        """
        This implementation filters the input table using a SQL statement and
        returns the filtered table and execution stats
        :param table: input table
        :return: list of output tables and custom statistics
        """

        # move table under a different name, to avoid SQL query parsing error
        input_table = table
        # execute prepared SQL query
        try:
            filtered_table = duckdb.execute(self.sql_statement, self.sql_params_dict).arrow()
        except Exception as ex:
            logger.error(f"FilterTransform::transform failed: {ex}")
            raise ex

        metadata = {
            "total_docs_count": input_table.num_rows,
            "total_bytes_count": input_table.nbytes,
            "filtered_docs_count": filtered_table.num_rows,
            "filtered_bytes_count": filtered_table.nbytes,
        }
        return [filtered_table], metadata


class FilterTransformConfiguration(DefaultTableTransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        global short_name
        super().__init__(name=short_name, transform_class=FilterTransform)
        self.params = {}

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the FilterTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """

        parser.add_argument(
            f"--{sql_statement_cli_param}",
            type=str,
            required=True,
            help="SQL statement used for filtering",
        )

        parser.add_argument(
            f"--{sql_params_dict_cli_param}",
            type=ast.literal_eval,
            required=False,
            default=ast.literal_eval("{}"),
            help="AST string containing SQL statement parameters.\n",  # + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        # help_example_dict = {
        #     "$lang_score": 0.5,
        #     "$perplexity_score": 520.0,
        # }

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=FilterTransformConfiguration())
    logger.info("Launching filtering")
    launcher.launch()
