# This helps to be able to run the test from within an IDE which seems to use the location of the
# file as the working directory.

import os

import pyarrow as pa
from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.test_support.transform import NOOPTransformConfiguration
from noop_transform import sleep_cli_param


class TestRayNOOPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = [(NOOPTransformConfiguration(), {sleep_cli_param: 0}, basedir + "/input", basedir + "/expected")]
        return fixtures
