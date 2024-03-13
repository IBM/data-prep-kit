from typing import Any

import pyarrow as pa


class AbstractTableTransform:
    """
    Converts input to output table
    Sub-classes must provide the transform() method to provide the conversion of one table to 0 or more new tables.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Converts input table into an output table.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :param table: input table
        :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
        propagated to metadata
        """
        raise NotImplemented()

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics. The majority of transformers
        should use default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
        propagated to metadata
        """
        return [], {}
