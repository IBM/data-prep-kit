from typing import Tuple

import blocklist_transform
import pyarrow as pa
from blocklist_transform import BlockListTransformConfiguration
from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.test_support.transform import NOOPTransformConfiguration


table = pa.Table.from_pydict({"name": pa.array(["Tom"]), "age": pa.array([23])})
expected_table = table  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestRayBlocklistTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        basedir = "test-data/"
        config = {blocklist_transform.blocked_domain_list_url_key: basedir + "domains/arjel"}
        fixtures = [(BlockListTransformConfiguration(), config, basedir + "input", basedir + "expected")]
        return fixtures
