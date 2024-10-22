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

import pyarrow as pa


table = pa.Table.from_pydict(
    {
        "contents": pa.array(
            [
                "My name is Tom chandler. Captain of the ship",
                "I work at Apple and I like to eat apples",
                "My email is tom@chadler.com and dob is 31.05.1987",
            ]
        ),
        "doc_id": pa.array(["doc1", "doc2", "doc3"]),
    }
)
expected_table = table.add_column(
    0,
    "transformed_contents",
    [
        [
            "My name is <PERSON>. Captain of the ship",
            "I work at <ORGANIZATION> and I like to eat apples",
            "My email is <EMAIL_ADDRESS> and dob is <DATE_TIME>",
        ]
    ],
)
detected_pii = [["PERSON"], ["ORGANIZATION"], ["EMAIL_ADDRESS", "DATE_TIME"]]
expected_table = expected_table.add_column(0, "detected_pii", [detected_pii])

redacted_expected_table = table.add_column(
    0,
    "transformed_contents",
    [
        ["My name is . Captain of the ship", "I work at  and I like to eat apples", "My email is  and dob is "],
    ],
)
redacted_expected_table = redacted_expected_table.add_column(0, "detected_pii", [detected_pii])
expected_metadata_list = [
    {"original_table_rows": 3, "original_column_count": 2, "transformed_table_rows": 3, "transformed_column_count": 4},
    {},
]
