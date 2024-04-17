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

import pyarrow as pa
import os
from blocklist_transform import (
    BlockListTransform,
    BlockListTransformConfiguration,
    annotation_column_name_cli_param,
    annotation_column_name_default,
    blocked_domain_list_path_cli_param,
    source_column_name_default,
    source_url_column_name_cli_param,
)
from data_processing.ray.transform_runtime import get_transform_config
from data_processing.test_support.transform import AbstractTransformTest


class TestBlockListTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            # When running outside the Ray orchestrator and its DataAccess/Factory, there is
            # no Runtime class to load the domains and the Transform must do it itself using
            # the blocklist_local_config for this test.
            f"--{blocked_domain_list_path_cli_param}",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/domains/arjel")),
            f"--{annotation_column_name_cli_param}",
            annotation_column_name_default,
            f"--{source_url_column_name_cli_param}",
            source_column_name_default,
        ]

        # Use the BlockListTransformConfiguration to compute the config parameters
        bltc = BlockListTransformConfiguration()
        config = get_transform_config(bltc, cli)

        fixtures = [
            (
                BlockListTransform(config),
                [self.input_df],
                [self.expected_output_df],
                self.expected_metadata_list,
            ),
        ]
        return fixtures

    # test data
    titles = pa.array(
        [
            "https://poker",
            "https://poker.fr",
            "https://poker.foo.bar",
            "https://abc.efg.com",
            "http://asdf.qwer.com/welcome.htm",
            "http://aasdf.qwer.com/welcome.htm",
            "https://zxcv.xxx/index.asp",
        ]
    )
    names = ["title"]
    input_df = pa.Table.from_arrays([titles], names=names)
    # poker
    # poker.fr
    # poker.foo.bar

    block_list = pa.array(
        [
            "poker",
            "poker.fr",
            "poker.foo.bar",
            "",
            "",
            "",
            "",
        ]
    )
    names1 = ["title", "blocklisted"]
    expected_output_df = pa.Table.from_arrays([titles, block_list], names=names1)
    expected_metadata_list = [
        {
            "total_docs_count": 7,
            "block_listed_docs_count": 3,
        },  # transform() metadata
        {},  # Empty flush() metadata
    ]


if __name__ == "__main__":
    t = TestBlockListTransform()
