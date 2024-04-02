import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
from resize_transform import ResizeTransformConfiguration


class TestRayResizeTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        # The following based on 3 identical input files of about 39kbytes, and 200 rows
        fixtures = []
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))

        # Split into 4 or so files
        config = {"max_rows_per_table": 125}
        fixtures.append((ResizeTransformConfiguration(), config, basedir + "/input", basedir + "/expected-rows-125"))

        # # Merge into 2 or so files
        config = {"max_rows_per_table": 300}
        fixtures.append((ResizeTransformConfiguration(), config, basedir + "/input", basedir + "/expected-rows-300"))

        # # Merge all into a single table
        config = {"max_mbytes_per_table": 1}
        fixtures.append((ResizeTransformConfiguration(), config, basedir + "/input", basedir + "/expected-mbytes-1"))

        # # Merge the 1st 2 and some of the 2nd with the 3rd
        config = {"max_mbytes_per_table": 0.05}
        fixtures.append(
            (ResizeTransformConfiguration(), config, basedir + "/input", basedir + "/expected-mbytes-0.05")
        )

        # Split into 4 or so files
        config = {"max_mbytes_per_table": 0.02}
        fixtures.append(
            (ResizeTransformConfiguration(), config, basedir + "/input", basedir + "/expected-mbytes-0.02")
        )

        return fixtures
