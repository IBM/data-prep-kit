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

from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from pii_redactor_transform import (
    PIIRedactorTransform,
    doc_transformed_contents_key,
    redaction_operator_key,
)
from test_data import expected_metadata_list, redacted_expected_table, table


class TestPIIRedactTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        fixtures = [
            (
                PIIRedactorTransform(
                    {doc_transformed_contents_key: doc_transformed_contents_key, redaction_operator_key: "redact"}
                ),
                [table],
                [redacted_expected_table],
                expected_metadata_list,
            ),
        ]
        return fixtures
