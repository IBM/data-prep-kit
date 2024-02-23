from typing import Any

import pyarrow as pa


class AbstractTableTransform:
    """
    Converts input to output table
    Sub-classes must provide the transform() method to provide the conversion of one table to 0 or more new tables.
    """

    def __init__(self, config: dict[str, Any]):
        """
        By convention and in support of the ray runtime, we enable the transform to be initialized with a
        transform-specific dictionary of configuration values.  The CLI arg definitions are provided
        elsewhere to allow for the arguments to be parsed in a process separate from where this instance
        is create.  This is because instance creation may include heavy lifting such as model loading.
        """

        pass

    def transform(self, table: pa.Table) -> list[pa.Table]:
        """
        Converts input table into zero or more output tables.
        :param table: input table
        :return: a list of 0 or more converted tables.
        """
        raise NotImplemented()
