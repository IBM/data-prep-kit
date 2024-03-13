import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from noop_transform import NOOPTransform


table = pa.Table.from_pydict({"name": pa.array(["Tom"]), "age": pa.array([23])})
expected_table = table  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestNOOPTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        fixtures = [
            (NOOPTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
            (NOOPTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
