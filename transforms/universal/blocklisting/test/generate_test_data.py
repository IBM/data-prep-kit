from data_processing.data_access import DataAccessLocal
from test_blocklist import TestBlockListTransform


if __name__ == "__main__":
    t = TestBlockListTransform()
    inp = t.input_df.to_arrow()
    out = t.expected_output_df.to_arrow()
    config = {"input_folder": "../test-data", "output_folder": "../test-data"}
    data_access = DataAccessLocal(config, [], False, -1)
    data_access.save_table("input/test1.parquet", inp)
    data_access.save_table("expected/test1.parquet", out)
