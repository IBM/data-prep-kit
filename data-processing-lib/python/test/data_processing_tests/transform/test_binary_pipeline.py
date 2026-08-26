# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import pathlib
from typing import Any, Tuple

import pyarrow as pa
from data_processing.test_support.transform import AbstractBinaryTransformTest
from data_processing.test_support.transform.noop_transform import NOOPTransform
from data_processing.transform import AbstractBinaryTransform
from data_processing.transform.pipelined_transform import (
    PipelinedTransform,
    transform_key,
)
from data_processing.utils import TransformUtils


table = pa.Table.from_pydict({"name": pa.array(["Tom", "Dick", "Harry"]), "age": pa.array([0, 1, 2])})
expected_table = table  # We only use NOOP

# Because the test is calling transform/flush_binary(), we get the additional metadata *_doc_count.
expected_metadata_list = [
    {"nfiles": 1, "nrows": 3, "result_doc_count": 3, "source_doc_count": 3},  # transform() result
    {}  # flush() result
    # {"result_doc_count": 0},  # flush_binary() result
]


class DoublerTransform(AbstractBinaryTransform):
    def __init__(self):
        self.extension = None
        self.buffer = []

    def transform_binary(self, file_name: str, byte_array: bytes) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        self.extension = pathlib.Path(file_name).suffix
        the_tuple = (byte_array, self.extension)
        self.buffer.append(the_tuple)
        return [the_tuple], {}

    def flush_binary(self) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        r = self.buffer
        self.buffer = None
        return r, {}


class TestPipelinedBinaryTransform(AbstractBinaryTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        # Defines correct evaluation of pipeline for the expected number of tables produced.
        # It does NOT test the transformation of the transform contained in the pipeline other
        # than to make sure the byte arrays are not changed due to using a NoopTransform in the pipeline.
        # .parquet is used as the extension because the transforms being used are AbstractTableTransforms
        # which use/expect parquet files.
        fixtures = []
        noop0 = NOOPTransform({"sleep": 0})
        noop1 = NOOPTransform({"sleep": 0})
        config = {transform_key: [noop0]}
        binary_table = TransformUtils.convert_arrow_to_binary(table)
        binary_expected_table = TransformUtils.convert_arrow_to_binary(expected_table)

        # Simple test to makes sure a single transform works
        fixtures.append(
            (
                PipelinedTransform(config),
                [("foo.parquet", binary_table)],
                [(binary_expected_table, ".parquet")],
                expected_metadata_list,
            )
        )

        # Put two transforms together
        config = {transform_key: [noop0, noop1]}
        fixtures.append(
            (
                PipelinedTransform(config),
                [("foo.parquet", binary_table)],
                [(binary_expected_table, ".parquet")],
                expected_metadata_list,
            )
        )

        # Add a transform to the pipeline that a) produces muliple tables and b) uses flush() to do it.
        config = {transform_key: [noop0, DoublerTransform()]}
        fixtures.append(
            (
                PipelinedTransform(config),
                [("foo.parquet", binary_table)],
                [(binary_expected_table, ".parquet"), (binary_expected_table, ".parquet")],
                expected_metadata_list,
            )
        )
        return fixtures
