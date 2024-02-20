from data_processing.data_access.data_access_lh import DataAccessLakeHouse

s3_cred = {
    "access_key": "e49c0337161a424bb77061a23e5a853e",
    "secret_key": "8431539f43d6df45c6acc39bee45ef291ac86188b3503ee3",
    "cos_url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "academic.ieee",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "academic.ieee.lh_unittest",
    "output_path": "lh-test/tables/academic/ieee/lh_unittest",
    "token": "eyJhbGciOiJIUzI1NiIsInppcCI6IkRFRiJ9.eJyrVkrNTczMUbJSqixNzEvOyHQoLdbLTMrVS87PVdJRKi5NwiWVmViiZGVobmBiaWhpbmKio5RaUQASMDQyszQFCtQCAGYOG84.oYILibFEhUzVTvmk9a4l-xBeZ4PIg237lDrvKlvl6Is",
}


def test_table_read_write():
    """
    Testing table read/write
    :return: None
    """
    # create data access
    d_a = DataAccessLakeHouse(
        s3_credentials=s3_cred, lakehouse_config=lakehouse_config, d_sets=None, checkpoint=False, m_files=-1
    )

    input_location = (
        "lh-test/tables/academic/ieee/data/version=0.0.1/"
        "language=en/00000-1-345d10e3-ed3c-46b3-8f0d-cb81af19898b-00001.parquet"
    )
    # read the table
    r_table = d_a.get_table(path=input_location)
    r_columns = r_table.column_names
    print(f"number of columns in the read table {len(r_columns)}, number of rows {r_table.num_rows}")
    assert 6220 == r_table.num_rows
    assert 14 == len(r_columns)
    # get table output location
    output_location = d_a.get_output_location(input_location)
    print(f"Output location {output_location}")
    assert (
        "lh-test/tables/academic/ieee/lh_unittest/data/version=0.0.1/"
        "language=en/00000-1-345d10e3-ed3c-46b3-8f0d-cb81af19898b-00001.parquet" == output_location
    )
    # save the table
    l, result = d_a.save_table(path=output_location, table=r_table)
    print(f"length of saved table {l}, result {result}")
    assert 220549646 == l
    s_columns = d_a.get_table(output_location).column_names
    assert len(r_columns) == len(s_columns)
    assert r_columns == s_columns


def test_get_folder():
    """
    Testing get folder
    :return: None
    """
    # create data access
    d_a = DataAccessLakeHouse(
        s3_credentials=s3_cred, lakehouse_config=lakehouse_config, d_sets=None, checkpoint=False, m_files=-1
    )
    # get the folder
    files = d_a.get_files_to_process()
    print(f"got {len(files[0])} files")
    assert 14 == len(files[0])


def test_get_todo_list():
    """
    Testing get_input_missing_from_output
    : return: None
    """
    # create data access
    d_a = DataAccessLakeHouse(
        s3_credentials=s3_cred, lakehouse_config=lakehouse_config, d_sets=None, checkpoint=False, m_files=-1
    )


#    print(f"got {len(d_a.get_input_missing_from_output())} files to process")
#    assert 12 == len(d_a.get_input_missing_from_output())


def test_files_to_process():
    """
    Testing get files to process
    :return: None
    """
    # create data access
    path_conf = {
        "input_folder": "lh-test/tables/academic/ieee/data/version=0.0.1/language=en/",
        "output_folder": "lh-test/tables/academic/ieee/lh_unittest/data/version=0.0.1/language=en/",
    }
    d_a = DataAccessLakeHouse(
        s3_credentials=s3_cred, lakehouse_config=lakehouse_config, d_sets=None, checkpoint=False, m_files=-1
    )
    # get files to process
    # files, profile = d_a.get_files_to_process(d_sets=None, checkpoint=False)
    files, profile = d_a.get_files_to_process()
    print(f"files {len(files)}, profile {profile}")
    assert 14 == len(files)
    assert 344.0891418457031 == profile["max_file_size"]
    assert 0.00907135009765625 == profile["min_file_size"]
    assert 1794.700538635254 == profile["total_file_size"]

    """
    # use checkpoint
    # files, profile = d_a.get_files_to_process(d_sets=None, checkpoint=True)
    files, profile = d_a.get_files_to_process()
    print(f"files with checkpointing {len(files)}, profile {profile}")
    assert 14 == len(files)
    assert 182.44072341918945 == profile["max_file_size"]
    assert 5.962291717529297 == profile["min_file_size"]
    assert 561.1689929962158 == profile["total_file_size"]
    # using data sets
    files, profile = d_a.get_files_to_process(d_sets=["dataset=textbooks"], checkpoint=False)
    print(f"using data sets files {len(files)}, profile {profile}")
    assert 9 == len(files)
    assert 320.3226261138916 == profile["max_file_size"]
    assert 5.962291717529297 == profile["min_file_size"]
    assert 881.4916191101074 == profile["total_file_size"]
    # using data sets with checkpointing
    files, profile = d_a.get_files_to_process(d_sets=["dataset=textbooks"], checkpoint=True)
    print(f"using data sets files {len(files)}, profile {profile}")
    assert 8 == len(files)
    assert 182.44072341918945 == profile["max_file_size"]
    assert 5.962291717529297 == profile["min_file_size"]
    assert 561.1689929962158 == profile["total_file_size"]
    """


if __name__ == "__main__":
    test_table_read_write()
    test_get_folder()
    test_files_to_process()
    test_get_todo_list()
