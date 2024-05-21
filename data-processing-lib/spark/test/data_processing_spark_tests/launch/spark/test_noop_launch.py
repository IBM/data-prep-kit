import os

from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_spark.runtime.spark.spark_launcher import SparkTransformLauncher
from data_processing_spark.test_support.transform.noop_transform import (
    NOOPSparkRuntimeConfiguration,
    NOOPTransformConfiguration,
)


df = None  # TBD
expected_table = df  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestRayNOOPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../../../../test-data/data_processing/spark/noop/"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        launcher = SparkTransformLauncher(NOOPSparkRuntimeConfiguration())
        fixtures = [(launcher, {"noop_sleep_sec": 0}, basedir + "/input", basedir + "/expected")]
        return fixtures
