import string
import sys
from typing import Any
import pyarrow as pa
import mmh3
import hashlib
from data_processing.utils import get_logger

logger = get_logger(__name__)


RANDOM_SEED = 42
LOCAL_TO_DISK = 2


class TransformUtils:
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
                size += TransformUtils.deep_get_size(element)
        if isinstance(ob, dict):
            for k, v in ob.items():
                size += TransformUtils.deep_get_size(k)
                size += TransformUtils.deep_get_size(v)
        return size

    @staticmethod
    def normalize_string(doc: str) -> str:
        """
        Normalize string
        :param doc: string to normalize
        :return: normalized string
        """
        return doc.replace(" ", "").replace("\n", "").lower().translate(str.maketrans("", "", string.punctuation))

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
        return mmh3.hash(s, seed=RANDOM_SEED, signed=False)

    @staticmethod
    def validate_columns(table: pa.Table, required: list[str]) -> bool:
        """
        Check if required columns exist in the table
        :param table: table
        :param required: list of required columns
        :return: true, if all columns exist, false otherwise
        """
        columns = table.schema.names
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
        if name in table.schema.names:
            table = table.drop(columns=[name])
        # append column
        return table.append_column(field_=name, column=[content])

    @staticmethod
    def verify_no_duplicate_columns(table: pa.Table, file: str) -> bool:
        """
        Verify that resulting table does not have duplicate columns
        :param table: table
        :param file: file where we are saving the table
        :return: True, if there are no duplicates, False otherwise
        """
        columns_list = table.schema.names
        columns_set = set(columns_list)
        if len(columns_set) != len(columns_list):
            logger.warning(f"Resulting table for file {file} contains duplicate columns {columns_list}. Skipping")
            return False
        return True

    @staticmethod
    def clean_path(path: str) -> str:
        """
        Clean path parameters:
            Removes white spaces from the input/output paths
            Removes schema prefix (s3://, http:// https://), if exists
            Adds the "/" character at the end, if it doesn't exist
            Removes URL encoding
        :param path: path to clean up
        :return: clean path
        """
        path = path.strip()
        if path == "":
            return path
        from urllib.parse import unquote, urlparse, urlunparse

        # Parse the URL
        parsed_url = urlparse(path)
        if parsed_url.scheme in ["http", "https"]:
            # Remove host
            parsed_url = parsed_url._replace(netloc="")
            parsed_url = parsed_url._replace(path=parsed_url.path[1:])

        # Remove the schema
        parsed_url = parsed_url._replace(scheme="")

        # Reconstruct the URL without the schema
        url_without_schema = urlunparse(parsed_url)

        # Remove //
        if url_without_schema[:2] == "//":
            url_without_schema = url_without_schema.replace("//", "", 1)

        return_path = unquote(url_without_schema)
        if return_path[-1] != "/":
            return_path += "/"
        return return_path


