import blocklist_transform
import pyarrow as pa
from blocklist_transform import BlockListTransform
from data_processing.data_access import DataAccessLocal
from data_processing.test_support.transform import AbstractTransformTest


class TestBlockListTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        config = {blocklist_transform.blocked_domain_list_path_key: "../test-data/domains/arjel"}
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
    inp = t.input_df.to_arrow()
    out = t.expected_output_df.to_arrow()
    config = {"input_folder": "/tmp", "output_folder": "./test-data"}
    data_access = DataAccessLocal(config, [], False, -1)
    data_access.save_table("../test-data/input/", inp)
    data_access.save_table("../test-data/expected/", out)
