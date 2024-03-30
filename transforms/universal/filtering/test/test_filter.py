import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
from data_processing.ray.transform_runtime import get_transform_config
from data_processing.test_support.transform import AbstractTransformTest
from data_processing.utils import ParamsUtils
from filter_transform import (
    FilterTransform,
    FilterTransformConfiguration,
    filter_columns_to_drop_cli_param,
    filter_columns_to_drop_default,
    filter_columns_to_drop_key,
    filter_criteria_cli_param,
    filter_criteria_default,
    filter_criteria_key,
    filter_logical_operator_cli_param,
    filter_logical_operator_default,
    filter_logical_operator_key,
)


class TestFilterTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            f"--{filter_criteria_cli_param}",
            """["docq_total_words > 100 AND docq_total_words < 200", "ibmkenlm_docq_perplex_score < 230"]""",
            f"--{filter_logical_operator_cli_param}",
            filter_logical_operator_default,
            f"--{filter_columns_to_drop_cli_param}",
            """["extra", "cluster"]""",
        ]

        # Use the FilterTransformConfiguration to compute the config parameters
        ftc = FilterTransformConfiguration()
        config = get_transform_config(ftc, cli)

        fixtures = [
            (
                FilterTransform(config),
                [self.input_df],
                [self.expected_output_df],
                self.expected_metadata_list,
            ),
        ]
        return fixtures

    input_file_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../test-data/", "input", "test1.parquet")
    )
    input_df = pq.read_table(input_file_path)

    expected_output_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../test-data", "expected", "test1-local")
    )

    expected_output_dataframe_file_path = os.path.abspath(os.path.join(expected_output_dir, "test1.parquet"))
    expected_output_df = pq.read_table(expected_output_dataframe_file_path)

    expected_output_metadata_file_path = os.path.abspath(os.path.join(expected_output_dir, "metadata.json"))
    with open(expected_output_metadata_file_path, "r") as meta_file:
        expected_metadata = json.load(meta_file)

    expected_metadata_list = [expected_metadata, {}]


if __name__ == "__main__":
    t = TestFilterTransform()
