import os

from data_processing.data_access import DataAccessLocal
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from pyspark.sql import SparkSession


class AbstractSparkTransformLauncherTest(AbstractTransformLauncherTest):
    def _validate_directory_contents_match(self, dir: str, expected: str):
        # we assume launch is completed and stopped,
        # therefore we are creating a new session and want to stop it below
        spark = SparkSession.builder.getOrCreate()
        dal = DataAccessLocal()
        test_generated_files = dal._get_all_files_ext(dir, [".parquet"])
        expected_files = dal._get_all_files_ext(expected, [".parquet"])
        result_df = spark.read.parquet(test_generated_files)
        expected_df = spark.read.parquet(expected_files)
        result_length = result_df.count()
        expected_length = expected_df.count()
        spark.stop()
        assert result_length == expected_length
