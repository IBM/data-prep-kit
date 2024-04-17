# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os

from data_processing.data_access import DataAccessS3
from moto import mock_aws


s3_cred = {
    "access_key": "access",
    "secret_key": "secret",
    "region": "us-east-1",
}

s3_conf = {
    "input_folder": "test/table_read_write/input/",
    "output_folder": "test/table_read_write/output/",
}


def _create_and_populate_bucket(d_a: DataAccessS3, input_location: str, n_files: int) -> None:
    # create bucket
    d_a.arrS3.s3_client.create_bucket(Bucket="test")
    # upload file
    loc = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../test-data/data_processing/input/sample1.parquet")
    )
    with open(loc, "rb") as file:
        bdata = file.read()
    for i in range(n_files):
        d_a.save_file(path=f"{input_location}sample{i}.parquet", data=bdata)


def test_table_read_write():
    """
    Testing table read/write
    :return: None
    """
    with mock_aws():
        # create data access
        d_a = DataAccessS3(s3_credentials=s3_cred, s3_config=s3_conf, d_sets=None, checkpoint=False, m_files=-1)
        # populate bucket
        input_location = "test/table_read_write/input/"
        _create_and_populate_bucket(d_a=d_a, input_location=input_location, n_files=1)
        # read the table
        input_location = f"{input_location}sample0.parquet"
        r_table = d_a.get_table(path=input_location)
        r_columns = r_table.column_names
        print(f"\nnumber of columns in the read table {len(r_columns)}, number of rows {r_table.num_rows}")
        assert 5 == r_table.num_rows
        assert 38 == len(r_columns)
        # get table output location
        output_location = d_a.get_output_location(input_location)
        print(f"Output location {output_location}")
        assert "test/table_read_write/output/sample0.parquet" == output_location
        # save the table
        l, result = d_a.save_table(path=output_location, table=r_table)
        print(f"length of saved table {l}, result {result}")
        assert 36132 == l
        s_columns = d_a.get_table(output_location).column_names
        assert len(r_columns) == len(s_columns)
        assert r_columns == s_columns


def test_get_folder():
    """
    Testing get folder
    :return: None
    """
    with mock_aws():
        # create data access
        d_a = DataAccessS3(s3_credentials=s3_cred, d_sets=None, checkpoint=False, m_files=-1)
        # populate bucket
        input_location = "test/table_read_write/input/"
        _create_and_populate_bucket(d_a=d_a, input_location=input_location, n_files=3)
        # get the folder
        files = d_a.get_folder_files(path=input_location, extensions=["parquet"])
        print(f"\ngot {len(files)} files")
        assert 3 == len(files)


def test_files_to_process():
    """
    Testing get files to process
    :return: None
    """
    with mock_aws():
        # create data access
        d_a = DataAccessS3(s3_credentials=s3_cred, s3_config=s3_conf, d_sets=None, checkpoint=False, m_files=-1)
        # populate bucket
        _create_and_populate_bucket(d_a=d_a, input_location=f"{s3_conf['input_folder']}dataset=d1/", n_files=4)
        _create_and_populate_bucket(d_a=d_a, input_location=f"{s3_conf['input_folder']}dataset=d2/", n_files=4)
        # get files to process
        files, profile = d_a.get_files_to_process()
        print(f"\nfiles {len(files)}, profile {profile}")
        assert 8 == len(files)
        assert 0.034458160400390625 == profile["max_file_size"]
        assert 0.034458160400390625 == profile["min_file_size"]
        assert 0.275665283203125 == profile["total_file_size"]
        # use checkpoint
        # populate bucket
        _create_and_populate_bucket(d_a=d_a, input_location=f"{s3_conf['output_folder']}dataset=d2/", n_files=2)
        d_a.checkpoint = True
        files, profile = d_a.get_files_to_process()
        print(f"files with checkpointing {len(files)}, profile {profile}")
        assert 6 == len(files)
        assert 0.034458160400390625 == profile["max_file_size"]
        assert 0.034458160400390625 == profile["min_file_size"]
        assert 0.20674896240234375 == profile["total_file_size"]
        # using data sets
        d_a.checkpoint = False
        d_a.d_sets = ["dataset=d1"]
        files, profile = d_a.get_files_to_process()
        print(f"using data sets files {len(files)}, profile {profile}")
        assert 4 == len(files)
        assert 0.034458160400390625 == profile["max_file_size"]
        assert 0.034458160400390625 == profile["min_file_size"]
        assert 0.1378326416015625 == profile["total_file_size"]
        # using data sets with checkpointing
        d_a.checkpoint = True
        d_a.d_sets = ["dataset=d2"]
        files, profile = d_a.get_files_to_process()
        print(f"using data sets with checkpointing files {len(files)}, profile {profile}")
        assert 2 == len(files)
        assert 0.034458160400390625 == profile["max_file_size"]
        assert 0.034458160400390625 == profile["min_file_size"]
        assert 0.06891632080078125 == profile["total_file_size"]
