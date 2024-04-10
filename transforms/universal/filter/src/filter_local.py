import os

from data_processing.data_access import DataAccessLocal
from filter_transform import (
    FilterTransform,
    filter_columns_to_drop_key,
    filter_criteria_key,
    filter_logical_operator_key,
)


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}


filter_criteria = [
    "docq_total_words > 100 AND docq_total_words < 200",
    "ibmkenlm_docq_perplex_score < 230",
]
filter_logical_operator = "AND"
filter_columns_to_drop = ["extra", "cluster"]

filter_params = {
    filter_criteria_key: filter_criteria,
    filter_columns_to_drop_key: filter_columns_to_drop,
    filter_logical_operator_key: filter_logical_operator,
}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Filter transform needs a DataAccess to ready the domain list.
    data_access = DataAccessLocal(local_conf)
    # Create and configure the transform.
    transform = FilterTransform(filter_params)
    # Use the local data access to read a parquet table.
    table = data_access.get_table(os.path.join(input_folder, "test1.parquet"))
    print(f"input table has {table.num_rows} rows")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table has {table_list[0].num_rows} rows")
    print(f"output metadata : {metadata}")
