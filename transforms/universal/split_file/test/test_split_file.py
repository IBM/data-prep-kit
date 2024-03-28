from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest


# todo: copied from noop.  Needs attention to set real test data.
table = pa.Table.from_pydict({"name": pa.array(["Tom"]), "age": pa.array([23])})
expected_table = table
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestSplitFileTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        fixtures = [
            # todo: copied from noop.  Needs attention to set real test data.
            # (SplitFileTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
            # (SplitFileTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
