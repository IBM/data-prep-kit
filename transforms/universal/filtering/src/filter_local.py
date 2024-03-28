import os

from data_processing.data_access import DataAccessLocal

from transforms.universal.filtering.src.filter_transform import (
    FilterTransform,
    sql_params_dict_key,
    sql_statement_key,
)


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
filter_sql_statement = "SELECT * FROM input_table WHERE title='https://poker'"
filter_sql_params_dict = {}

filter_params = {
    sql_statement_key: filter_sql_statement,
    sql_params_dict_key: filter_sql_params_dict,
}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Filter transform needs a DataAccess to ready the domain list.
    data_access = DataAccessLocal(local_conf)
    # Create and configure the transform.
    transform = FilterTransform(filter_params)
    # Use the local data access to read a parquet table.
    table = data_access.get_table(os.path.join(input_folder, "test1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")
