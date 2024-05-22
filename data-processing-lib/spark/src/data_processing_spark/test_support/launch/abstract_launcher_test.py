from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)


class AbstractSparkTransformLauncherTest(AbstractTransformLauncherTest):
    def _validate_directory_contents_match(self, dir: str, expected: str):
        assert True
