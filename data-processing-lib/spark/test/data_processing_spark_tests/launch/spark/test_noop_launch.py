import os

from data_processing_spark.runtime.spark.spark_launcher import SparkTransformLauncher
from data_processing_spark.test_support.launch.abstract_launcher_test import (
    AbstractSparkTransformLauncherTest,
)
from data_processing_spark.test_support.transform.noop_transform import (
    NOOPSparkRuntimeConfiguration,
    NOOPTransformConfiguration,
)


df = None  # TBD
expected_table = df  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestSparkNOOPTransform(AbstractSparkTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        proj_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
        test_data_dir = os.path.join(proj_dir, "test-data/data_processing/spark/noop/")
        config_path = os.path.join(proj_dir, "config/spark_profile_local.yml")
        launcher = SparkTransformLauncher(NOOPSparkRuntimeConfiguration())
        cli_params = {
            "noop_sleep_sec": 0,
            "spark_local_config_filepath": config_path,
        }
        fixtures = [(launcher, cli_params, test_data_dir + "/input", test_data_dir + "/expected")]
        return fixtures
