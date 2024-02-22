import pyarrow as pa


class AbstractTableTransform:
    """
    Converts input to output table
    Sub-classes must provide the transform() method to provide the conversion of one table to 0 or more new tables.
    """

    def transform(self, table: pa.Table) -> list[pa.Table]:
        """
        Converts input table into an output table
        :param table: input table
        :return: a list of 0 or more converted tables.
        """
        raise NotImplemented()
