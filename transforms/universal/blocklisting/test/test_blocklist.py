from typing import Tuple

import blocklist_transform
import polars as pl
from blocklist_transform import BlockListTransform
from data_processing.data_access import DataAccessLocal
from data_processing.test_support.transform import AbstractTransformTest


class TestBlockListTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        config = {blocklist_transform.blocked_domain_list_url_key: "test-data/domains/arjel"}
        fixtures = [
            (
                BlockListTransform(config),
                [self.input_df.to_arrow()],
                [self.expected_output_df.to_arrow()],
                self.expected_metadata_list,
            ),
        ]
        return fixtures

    # test data
    input_df = pl.DataFrame(
        {
            "title": [
                "https://poker",
                "https://poker.fr",
                "https://poker.foo.bar",
                "https://abc.efg.com",
                "http://asdf.qwer.com/welcome.htm",
                "http://aasdf.qwer.com/welcome.htm",
                "https://zxcv.xxx/index.asp",
            ]
        }
    )
    # poker
    # poker.fr
    # poker.foo.bar

    expected_output_df = pl.DataFrame(
        {
            "title": [
                "https://poker",
                "https://poker.fr",
                "https://poker.foo.bar",
                "https://abc.efg.com",
                "http://asdf.qwer.com/welcome.htm",
                "http://aasdf.qwer.com/welcome.htm",
                "https://zxcv.xxx/index.asp",
            ],
            "url_blocklisting_refinedweb": [
                "poker",
                "poker.fr",
                "poker.foo.bar",
                "",
                "",
                "",
                "",
            ],
        }
    )

    expected_metadata_list = [
        {
            "total_docs_count": 7,
            "blocklisted_docs_count": 3,
        },  # transform() metadata
        {},  # Empty flush() metadata
    ]

    # input_df_2 = pl.DataFrame(
    #     {
    #         "title": [
    #             "https://asdf.com",
    #             "https://asdf.com/",
    #             "https://www.asdf.com/index.html",
    #             "https://qwer.com/contents.asp?x=1&y=2",
    #             "http://asdf.qwer.com/welcome.htm",
    #             "http://aasdf.qwer.com/welcome.htm",
    #             "https://zxcv.xxx/index.asp",
    #         ],
    #         "url_blocklisting_refinedweb": [
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #         ],
    #     }
    # )

    # # list of restricted domains
    # domain_blocklist = set(["asdf.com", "asdf.qwer.com", "xxx"])

    # def test_trie(self):
    #     trie = blocklist_utils.build_trie_struct(self.domain_blocklist)
    #     assert len(trie) == 3
    #     assert "xxx" in trie
    #     assert "com.asdf" in trie
    #     assert "com.qwer.asdf" in trie
    #     assert "com.qwer" not in trie

    # def test_blocklist(self):
    #     input_table = self.input_df.to_arrow()
    #     self.mutator.trie = blocklist_utils.build_trie_struct(self.domain_blocklist)
    #     out_table, metadata = self.mutator.mutate(input_table)
    #     out_df = pl.from_arrow(out_table)
    #     print(f"out_df = {out_df}")
    #     assert len(out_df) == 7
    #     blocklisted_docs_count = len(
    #         out_df.select(
    #             pl.col(self.mutator.blocklist_column_name).filter(pl.col(self.mutator.blocklist_column_name).ne(""))
    #         )
    #     )
    #     assert blocklisted_docs_count == 5
    #     assert out_df[0, 1] == "asdf.com"
    #     assert out_df[1, 1] == "asdf.com"
    #     assert out_df[2, 1] == "www.asdf.com"
    #     assert out_df[3, 1] == ""
    #     assert out_df[4, 1] == "asdf.qwer.com"
    #     assert out_df[5, 1] == ""
    #     assert out_df[6, 1] == "zxcv.xxx"
    #     print(json.dumps(metadata, indent=2, default=str))
    #     assert metadata.get("total_docs_count", 0) == 7
    #     assert metadata.get("blocklisted_docs_count", 0) == 5

    # def test_blocklist_result_column_already_in_place(self):
    #     input_table = self.input_df_2.to_arrow()
    #     self.mutator.trie = blocklist_utils.build_trie_struct(self.domain_blocklist)
    #     out_table, metadata = self.mutator.mutate(input_table)
    #     out_df = pl.from_arrow(out_table)
    #     print(f"out_df = {out_df}")
    #     assert len(out_df) == 7
    #     blocklisted_docs_count = len(
    #         out_df.select(
    #             pl.col(self.mutator.blocklist_column_name).filter(pl.col(self.mutator.blocklist_column_name).ne(""))
    #         )
    #     )
    #     assert blocklisted_docs_count == 5
    #     assert out_df[0, 1] == "asdf.com"
    #     assert out_df[1, 1] == "asdf.com"
    #     assert out_df[2, 1] == "www.asdf.com"
    #     assert out_df[3, 1] == ""
    #     assert out_df[4, 1] == "asdf.qwer.com"
    #     assert out_df[5, 1] == ""
    #     assert out_df[6, 1] == "zxcv.xxx"
    #     assert metadata.get("total_docs_count", 0) == 7
    #     assert metadata.get("blocklisted_docs_count", 0) == 5


if __name__ == "__main__":
    t = TestBlockListTransform()
    inp = t.input_df.to_arrow()
    out = t.expected_output_df.to_arrow()
    config = {"input_folder": "/tmp", "output_folder": "./test-data"}
    data_access = DataAccessLocal(config, [], False, -1)
    data_access.save_table("../test-data/input/", inp)
    data_access.save_table("../test-data/expected/", out)
