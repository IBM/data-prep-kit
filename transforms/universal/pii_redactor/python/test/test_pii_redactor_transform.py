import pyarrow as pa
from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from pii_redactor_transform import PIIRedactorTransform, doc_contents_key


table = pa.Table.from_pydict(
    {
        "contents": pa.array(
            [
                "My name is Tom chandler. Captain of the ship",
                "I work at Apple " "and I like to " "eat apples",
                "My email is tom@chadler.com and dob is 31.05.1987",
            ]
        ),
        "doc_id": pa.array(["doc1", "doc2", "doc3"]),
    }
)
expected_table = table.add_column(
    0,
    "new_contents",
    [
        [
            "My name is <PERSON>. Captain of the ship",
            "I work at <ORGANIZATION> and I like to eat apples",
            "My email is <EMAIL_ADDRESS> and dob is <DATE_TIME>",
        ]
    ],
)  # We're a noop after all.
expected_metadata_list = [
    {"original_table_rows": 3, "original_column_count": 2, "transformed_table_rows": 3, "transformed_column_count": 3},
    {},
]  # transform() result  # flush() result


class TestPIIRedactTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        fixtures = [
            (
                PIIRedactorTransform({doc_contents_key: doc_contents_key}),
                [table],
                [expected_table],
                expected_metadata_list,
            ),
        ]
        return fixtures
