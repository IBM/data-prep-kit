import os

from blocklist_transform import (
    BlockListTransform,
    annotation_column_name_key,
    blocked_domain_list_path_key,
    source_url_column_name_key,
)
from data_processing.data_access import DataAccessLocal


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
blocklist_conf_url = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/domains"))
blocklist_annotation_column_name = "blocklisted"
blocklist_doc_source_url_column = "title"

block_list_params = {
    blocked_domain_list_path_key: blocklist_conf_url,
    annotation_column_name_key: blocklist_annotation_column_name,
    source_url_column_name_key: blocklist_doc_source_url_column,
}
if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Blocklist transform needs a DataAccess to ready the domain list.
    data_access = DataAccessLocal(local_conf)
    block_list_params["data_access"] = data_access
    # Create and configure the transform.
    transform = BlockListTransform(block_list_params)
    # Use the local data access to read a parquet table.
    table = data_access.get_table(os.path.join(input_folder, "test1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")
