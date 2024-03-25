import pyarrow as pa
from data_processing.data_access import DataAccessLocal
from data_processing.ray.transform_runtime import get_transform_config
from data_processing.test_support.transform import AbstractTransformTest
from data_processing.utils import ParamsUtils
from langselect_transform import (
    LangSelectorTransform,
    LangSelectorTransformConfiguration,
    lang_allowed_langs_file_key,
    lang_known_selector,
    lang_lang_column_key,
)


class TestLangSelectorTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            # When running outside the Ray orchestrator and its DataAccess/Factory, there is
            # no Runtime class to load the domains and the Transform must do it itself using
            # the lang_select_local_config for this test.
            f"--{lang_allowed_langs_file_key}",
            "../test-data/languages/allowed-code-languages.txt",
            f"--{lang_lang_column_key}",
            "language",
            f"--{lang_known_selector}",
            "True",
            "--lang_select_local_config",
            ParamsUtils.convert_to_ast({"input_folder": "/tmp", "output_folder": "/tmp"}),
        ]

        # Use the BlockListTransformConfiguration to compute the config parameters
        lstc = LangSelectorTransformConfiguration()
        config = get_transform_config(lstc, cli)

        fixtures = [
            (
                LangSelectorTransform(config),
                [self.input_df],
                [self.expected_output_df],
                self.expected_metadata_list,
            ),
        ]
        return fixtures

    # test data
    languages = pa.array(
        [
            "Cobol",
            "Java",
            "C",
        ]
    )
    names = ["language"]
    input_df = pa.Table.from_arrays([languages], names=names)

    filtered = pa.array(
        [
            "Java",
            "C",
        ]
    )
    expected_output_df = pa.Table.from_arrays([filtered], names=names)
    expected_metadata_list = [
        {
            "supported languages": 2,
            "unsupported languages": 1,
        },  # transform() metadata
        {},  # Empty flush() metadata
    ]


if __name__ == "__main__":
    t = TestLangSelectorTransform()
