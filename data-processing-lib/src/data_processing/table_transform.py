import pyarrow

class AbstractTableTransform:
    """
    Converts input to output table
    Sub-classes must provide the filter() method to provide the conversion of one data frame to another.
    """

    def transform(self, table: pyarrow.Table) -> list[pyarrow.Table]:
        """
        converting input table into an output tablr
        :param table: input table
        :return: a list of converted tables to be written to the output
        """
        raise NotImplemented()


