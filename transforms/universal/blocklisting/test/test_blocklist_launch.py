from typing import Tuple

import pyarrow as pa
from blocklist_transform import (
    BlockListTransformConfiguration,
    blocked_domain_list_path_key,
)
from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.test_support.transform import NOOPTransformConfiguration


class TestRayBlocklistTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        basedir = "../test-data/"
        config = {blocked_domain_list_path_key: basedir + "domains/arjel"}
        fixtures = [(BlockListTransformConfiguration(), config, basedir + "input", basedir + "expected")]
        return fixtures
