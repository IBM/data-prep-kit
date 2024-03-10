import os
from blocklist_transform import (
    BlockListTransformConfiguration, blocked_domain_list_path_key, annotation_column_name_key,
    annotation_column_name_default, source_url_column_name_key, source_column_name_default
)
from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.utils import ParamsUtils


class TestRayBlocklistTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the bl_local_config configuration.
            "blocklist_local_config": ParamsUtils.convert_to_ast({"input_folder": "/tmp", "output_folder": "/tmp"}),
            blocked_domain_list_path_key:
                os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/domains/arjel")),
            annotation_column_name_key: annotation_column_name_default,
            source_url_column_name_key: source_column_name_default,
        }
        fixtures = [(BlockListTransformConfiguration(), config, basedir + "/input", basedir + "/expected")]
        return fixtures
