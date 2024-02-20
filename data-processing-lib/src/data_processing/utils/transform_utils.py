import hashlib
import string
import sys
from typing import Any

import mmh3
import pyarrow as pa


class FilterUtils:
    """
    Class implementing support methods for filter implementation
    """

    @staticmethod
    def deep_get_size(ob) -> int:
        """
        Getting the complete size of the Python object. Based on
        https://www.askpython.com/python/built-in-methods/variables-memory-size-in-python
        Supports Python structures: list, tuple and set
            :param ob: object
            :return: object size
        """
        size = sys.getsizeof(ob)
        if isinstance(ob, (list, tuple, set)):
            for element in ob:
                size += FilterUtils.deep_get_size(element)
        if isinstance(ob, dict):
            for k, v in ob.items():
                size += FilterUtils.deep_get_size(k)
                size += FilterUtils.deep_get_size(v)
        return size

    @staticmethod
    def str_to_hash(val: str) -> str:
        """
        compute string hash
        :param val: string
        :return: hash value
        """
        return hashlib.sha256(val.encode("utf-8")).hexdigest()

    @staticmethod
    def str_to_int(s: str) -> int:
        """
        Convert string to int using mmh3 hashing. Ensures predictable result by setting seed
        :param s: string
        :return: int hash
        """
        return mmh3.hash(s, seed=42, signed=False)

    @staticmethod
    def normalize_string(doc: str) -> str:
        """
        Normalize string
        :param doc: string to normalize
        :return: normalized string
        """
        return doc.replace(" ", "").replace("\n", "").lower().translate(str.maketrans("", "", string.punctuation))

    @staticmethod
    def validata_columns(table: pa.Table, required: list[str]) -> bool:
        """
        Check if required columns exist in the table
        :param table: table
        :param required: list of required columns
        :return: true, if all columns exist, false otherwise
        """
        columns = table.columns
        result = True
        for r in required:
            if r not in columns:
                result = False
                break
        if not result:
            print(f"Not all required columns are present in the table - required {required}, present {columns}")
        return result

    @staticmethod
    def add_column(table: pa.Table, name: str, content: list[Any]) -> pa.Table:
        """
        Add column to the table
        :param table: original table
        :param name: column name
        :param content: content of the column
        :return: updated table, containing new column
        """
        # check if column already exist and drop it
        if name in table.columns:
            table = table.drop(columns=[name])
        # append column
        return table.append_column(field_=name, column=[content])
