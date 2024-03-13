from io import BytesIO
from typing import Tuple
from unittest.mock import patch

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from antivirus_transform import AntivirusTransform


table = pa.Table.from_pydict({
            'document_id': ['ID_1', 'ID_2'],
            'contents': ['INNOCENT', 'VIRUS'],
        })
expected_table = pa.Table.from_pydict({
            'document_id': ['ID_1', 'ID_2'],
            'contents': ['INNOCENT', 'VIRUS'],
            'virus_detection': [None, 'VIRUS_DETECTION'],
        })
expected_metadata_list = [{"clean": 1, "infected": 1}, {}]  # transform() result  # flush() result

def side_effect(arg: BytesIO) -> dict:
    if arg.getvalue().decode() == 'VIRUS':
        return { 'stream': ('FOUND', 'VIRUS_DETECTION') }
    return { 'stream': ('OK', None) }


class TestAntivirusTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        fixtures = [
            (AntivirusTransform({"input_column": "contents", "output_column": "virus_detection"}), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
    
    def test_transform(
        self,
        transform: AntivirusTransform,
        in_table_list: list[pa.Table],
        expected_table_list: list[pa.Table],
        expected_metadata_list: list[dict[str, float]],
    ):
        with patch('clamd.ClamdUnixSocket', autospec=True) as MockClamdUnixSocket:
            mockInstance = MockClamdUnixSocket.return_value
            mockInstance.instream.side_effect = side_effect
            super().test_transform(
                transform,
                in_table_list,
                expected_table_list,
                expected_metadata_list
            )
