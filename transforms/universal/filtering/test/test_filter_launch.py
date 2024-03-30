import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
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


class TestRayFilterTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the bl_local_config configuration.
            "local_config": ParamsUtils.convert_to_ast(
                {
                    "input_folder": os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/")),
                    "output_folder": os.path.abspath(os.path.join(os.path.dirname(__file__), "../output")),
                }
            ),
            filter_criteria_cli_param: [
                "docq_total_words > 100 AND docq_total_words < 200",
                "ibmkenlm_docq_perplex_score < 230",
            ],
            filter_logical_operator_cli_param: filter_logical_operator_default,
            filter_columns_to_drop_cli_param: ["extra", "cluster"],
        }
        input_dir = os.path.join(basedir, "input")
        expected_dir = os.path.join(basedir, "expected", "test1")
        fixtures = [(FilterTransformConfiguration(), config, input_dir, expected_dir)]
        return fixtures
