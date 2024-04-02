import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
from resize_transform import ResizeTransformConfiguration


class TestRayResizeTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the bl_local_config configuration.
            "max_table_size": 1,
            #    "max_documents_table": 150
        }
        fixtures = [(ResizeTransformConfiguration(), config, basedir + "/input", basedir + "/expected")]
        return fixtures
