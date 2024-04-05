import os
import sys

from data_processing.data_access import DataAccessLocal
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from doc_id_transform import DocIDTransform, DocIDTransformConfiguration


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))

doc_id_params = {"doc_column": "contents", "hash_column": "doc_hash"}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Create and configure the transform.
    transform = DocIDTransform(doc_id_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    table = data_access.get_table(os.path.join(input_folder, "sample1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    table = table_list[0]
    print(f"\noutput table: {table}")
    print(f"output metadata : {metadata}")
    column = table["doc_hash"]
    print(f"hashed column : {column}")
    print(f"index column : {column}")
