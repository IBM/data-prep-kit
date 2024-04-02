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
        fixtures = []
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))

        fixtures.append(
            (
                FilterTransformConfiguration(),
                {
                    filter_criteria_cli_param: [
                        "docq_total_words > 100 AND docq_total_words < 200",
                        "ibmkenlm_docq_perplex_score < 230",
                    ],
                    filter_logical_operator_cli_param: filter_logical_operator_default,
                    filter_columns_to_drop_cli_param: ["extra", "cluster"],
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-and"),
            )
        )

        fixtures.append(
            (
                FilterTransformConfiguration(),
                {
                    filter_criteria_cli_param: [
                        "docq_total_words > 100 AND docq_total_words < 200",
                        "ibmkenlm_docq_perplex_score < 230",
                    ],
                    filter_logical_operator_cli_param: "OR",
                    filter_columns_to_drop_cli_param: ["extra", "cluster"],
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-or"),
            )
        )

        fixtures.append(
            (
                FilterTransformConfiguration(),
                {
                    filter_criteria_cli_param: [],
                    filter_logical_operator_cli_param: filter_logical_operator_default,
                    filter_columns_to_drop_cli_param: [],
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-default"),
            )
        )

        fixtures.append(
            (
                FilterTransformConfiguration(),
                {
                    filter_criteria_cli_param: [
                        "date_acquired BETWEEN '2023-07-04' AND '2023-07-08'",
                        "title LIKE 'https://%'",
                    ],
                    filter_logical_operator_cli_param: filter_logical_operator_default,
                    filter_columns_to_drop_cli_param: [],
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-datetime-like"),
            )
        )

        fixtures.append(
            (
                FilterTransformConfiguration(),
                {
                    filter_criteria_cli_param: [
                        "hash IN ('e3b9ede15e7f396da1e39b0dfb32ba6c', 'bc0ef9ddd11e90342a50f494f6293e8d', 'cf1a5cde8e8734c3c75eb5e1229c9b15')",  # pragma: allowlist secret
                    ],
                    filter_logical_operator_cli_param: filter_logical_operator_default,
                    filter_columns_to_drop_cli_param: [],
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-in"),
            )
        )

        return fixtures
