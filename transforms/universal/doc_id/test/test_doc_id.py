from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from data_processing.utils import TransformUtils
from doc_id_transform import (
    DocIDTransform,
    doc_column_name_key,
    hash_column_name_key,
    int_column_name_key,
)


table = pa.Table.from_pydict(
    {
        "doc": pa.array(["Tom", "Joe"]),
    }
)
expected_table = pa.Table.from_pydict(
    {
        "doc": pa.array(["Tom", "Joe"]),
        "doc_hash": pa.array([TransformUtils.str_to_hash("Tom"), TransformUtils.str_to_hash("Joe")]),
        "doc_int": pa.array([0, 1]),
    }
)
expected_metadata_list = [{}, {}]  # transform() result  # flush() result


class TestDocIDTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        fixtures = []
        config = {
            doc_column_name_key: "doc",
            hash_column_name_key: "doc_hash",
            int_column_name_key: "doc_int",
        }
        fixtures.append((DocIDTransform(config), [table], [expected_table], expected_metadata_list))
        return fixtures
