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

import os
import pyarrow as pa
from data_processing.test_support.transform.table_transform_test import AbstractTableTransformTest
from data_processing.transform import get_transform_config
from issues_transform import (
    IssuesTransform,
    IssuesTransformConfiguration
)

class TestIssuesTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            "--issues_events_column_name", "events",
            "--issues_min_chars", "200",
            "--issues_max_chars", "7000",
            "--issues_max_events", "10",
            "--issues_max_lines", "80",
        ]
        transformConfig = IssuesTransformConfiguration()
        config = get_transform_config(transformConfig, cli)
        table = pa.Table.from_arrays(
            [
                pa.array([[
                    {'author': 'hoge', 'comment': 'Comment: Spec is too rigid on requiring initial HAVE_CURRENT_DATA transition occur synchronously within coded frame processing', 'description': 'Description: Spec is too rigid on requiring initial HAVE_CURRENT_DATA transition occur synchronously within coded frame processing'}, 
                    {'author': 'hoge', 'comment': " Note, this is part of why Chrome is failing the wpt/media-source/mediasource-append-buffer.html's 'Test appendBuffer events order.' case.  See https://crbug.com/641121 for more info (there's a similar problem with initial HAVE_METADATA transition, but that may be more reasonable to retain in spec; though it also can delay 'updateend' and further media appending by app in at least Chrome and IIUC, Firefox."}, 
                    {'author': 'geho', 'description': ' Indeed, this is one area of the spec we do not follow.\r'},
                ]]),
            ],
            names=["events"],
        )
        expected_table = pa.Table.from_struct_array(pa.array([
            {
                "bot_issue": False,
                "event_count": 3,
                "events": [
                    {'author': 'hoge', 'masked_author': 'username_0', 'text': 'Comment: Spec is too rigid on requiring initial HAVE_CURRENT_DATA transition occur synchronously within coded frame processing'}, 
                    {'author': 'hoge', 'masked_author': 'username_0', 'text': "Note, this is part of why Chrome is failing the wpt/media-source/mediasource-append-buffer.html's 'Test appendBuffer events order.' case.  See https://crbug.com/641121 for more info (there's a similar problem with initial HAVE_METADATA transition, but that may be more reasonable to retain in spec; though it also can delay 'updateend' and further media appending by app in at least Chrome and IIUC, Firefox."}, 
                    {'author': 'geho', 'masked_author': 'username_1', 'text': 'Indeed, this is one area of the spec we do not follow.'},
                ],
                "modified_by_bot": False,
                "modified_usernames": False,
                "text_size": 588,
                "user_count": 2
            }
        ]))
        return [
            (
                IssuesTransform(config),
                [table],
                [expected_table],
                [{'size_before_filter': 1, 'size_gb_before_filter': 588, 'size_after_long_comments_truncate': 1, 'size_gb_after_long_comments_truncate': 588, 'size_after_bot_filter': 2, 'size_gb_after_bot_filter': 1176, 'size_after_users_filter': 0, 'size_gb_after_users_filter': 0}, {}],
            )
        ]
