# This helps to be able to run the test from within an IDE which seems to use the location of the
# file as the working directory.
import sys


sys.path.append("../src")

from typing import Tuple

import pyarrow as pa
from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.test_support.transform import NOOPTransformConfiguration


table = pa.Table.from_pydict({"name": pa.array(["Tom"]), "age": pa.array([23])})
expected_table = table  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestRayNOOPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        basedir = "../test-data/"
        fixtures = [(NOOPTransformConfiguration(), {"noop_sleep_sec": 0}, basedir + "input", basedir + "expected")]
        return fixtures
