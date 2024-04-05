import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.utils import ParamsUtils
from lang_annotator_transform import (
    LangSelectorTransformConfiguration,
    lang_allowed_langs_file_key,
    lang_known_selector,
    lang_lang_column_key,
    lang_output_column_key,
)


class TestRayLangSelectorTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../test-data")
        )
        languages_file = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "../test-data/languages/allowed-code-languages.txt",
            )
        )
        config = {
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the lang_select_local_config configuration.
            "lang_select_local_config": ParamsUtils.convert_to_ast(
                {"input_folder": "/tmp", "output_folder": "/tmp"}
            ),
            lang_allowed_langs_file_key: languages_file,
            lang_lang_column_key: "language",
            lang_output_column_key: "allowed_languages",
            lang_known_selector: True,
        }
        fixtures = [
            (
                LangSelectorTransformConfiguration(),
                config,
                basedir + "/input",
                basedir + "/expected",
            )
        ]
        return fixtures
