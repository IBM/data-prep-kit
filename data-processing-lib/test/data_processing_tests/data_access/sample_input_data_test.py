import os
from data_processing.data_access import DataAccessLocal

def test_table_sampling_data():
    """
    Testing data sampling
    :return: None
    """

    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                "../../../../transforms/universal/ededup/test-data/input"))
    output_folder = "/tmp"
    print(input_folder)
    data_access = DataAccessLocal({"input_folder": input_folder, "output_folder": output_folder})
    profile = data_access.sample_input_data()
    print(f"\nprofiled directory {input_folder}")
    print(f"profile {profile}")
    assert profile['estimated number of docs'] == 5.0
    assert profile['max_file_size'] <  0.035
    assert profile['min_file_size'] <  0.035
    assert profile['total_file_size'] <  0.035
    assert profile['average table size MB'] < 0.016



