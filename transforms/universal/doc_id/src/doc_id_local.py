import os
import sys

from data_processing.data_access import DataAccessLocal
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from doc_id_transform import DocIDTransform, DocIDTransformConfiguration


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
# worker_options = {"num_cpus": 0.8}
# code_location = {"github": "github", "commit_hash": "12345", "path": "path"}

doc_id_params = {"doc_column": "contents", "hash_column": "doc_hash"}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Create and configure the transform.
    transform = DocIDTransform(doc_id_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal(local_conf)
    table = data_access.get_table(os.path.join(input_folder, "sample1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    table = table_list[0]
    print(f"\noutput table: {table}")
    print(f"output metadata : {metadata}")
    column = table["doc_hash"]
    print(f"hashed column : {column}")
    column = table["doc_index"]
    print(f"index column : {column}")
