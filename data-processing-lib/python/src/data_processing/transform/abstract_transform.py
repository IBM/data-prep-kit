from typing import Any, Generic, TypeVar


DATA = TypeVar("DATA")


class AbstractTransform(Generic[DATA]):
    def transform(self, data: DATA, file_name: str = None) -> tuple[list[DATA], dict[str, Any]]:
        """
        Converts input table into an output table.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :param data: input table
        :param file_name: optional - name of the input file
        :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
        propagated to metadata
        """
        raise NotImplemented()
