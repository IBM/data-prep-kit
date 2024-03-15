import pyarrow as pa
from data_processing.data_access import DataAccessLocal
from data_processing.ray.transform_runtime import get_transform_config
from data_processing.test_support.transform import AbstractTransformTest
from data_processing.utils import ParamsUtils
from doc_quality_transform import DocQualityTransform, DocQualityTransformConfiguration


class TestDocQualityTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            # When running outside the Ray orchestrator and its DataAccess/Factory, there is
            # no Runtime class to load the domains and the Transform must do it itself using
            # the blocklist_local_config for this test.
            ParamsUtils.convert_to_ast({"input_folder": "/tmp", "output_folder": "/tmp"}),
        ]

        # Use the DocQualityTransformConfiguration to compute the config parameters
        bltc = DocQualityTransformConfiguration()
        config = get_transform_config(bltc, cli)

        fixtures = [
            (
                DocQualityTransform(config),
                [self.input_df],
                [self.expected_output_df],
                self.expected_metadata_list,
            ),
        ]
        return fixtures

    # test data
    document_ids = pa.array([1001])
    contents = pa.array(
        [
            "タッチダウンオフィスは、LANと電源を用意して他のオフィスからやってきた利用者が作業できる環境を整えた場所。 "
            "生産性の向上を目的に通常オフィスの内部に設けられる。 東京駅前に企業と契約して社員にタッチダウンオフィスを提供す"
            "るサービスを展開するデスカットのような施設もある。 余ったスペースにLANと電源を用意してつくった出張者席。 "
            "社内のバッファスペースとして位置付けていたが、オフィス面積の削減で営業社員に固定席を設定しないフリーアドレス制を導入したために、"
            "全部の席がタッチダウン化してしまう事もある。 たった2時間の会議の為に出張したものの、気になるのでメールだけでもずっとみていたい、"
            "という時には便利である。"
        ]
    )
    input_df = pa.Table.from_arrays([document_ids, contents], names=["document_id", "contents"])
    ft_lang = "ja"

    # copy expected_table from the input_df
    schema = input_df.schema
    data = input_df.to_pydict()
    expected_output_df = pa.Table.from_pydict(data, schema=schema)

    expected_output_df = expected_output_df.append_column("docq_symbol_to_word_ratio", pa.array([0.0]))
    expected_output_df = expected_output_df.append_column("docq_bullet_point_ratio", pa.array([0.0]))
    expected_output_df = expected_output_df.append_column("docq_ellipsis_line_ratio", pa.array([0.0]))
    expected_output_df = expected_output_df.append_column("docq_alphabet_word_ratio", pa.array([1.0]))
    expected_output_df = expected_output_df.append_column("docq_contain_common_en_words", pa.array([False]))
    expected_output_df = expected_output_df.append_column("metakenlm_docq_perplex_score", pa.array([177.9]))

    if ft_lang == "ja":
        expected_output_df = expected_output_df.append_column("docq_avg_ja_sentence_len", pa.array([51]))
        expected_output_df = expected_output_df.append_column("docq_first_ja_alphabet_pos", pa.array([0]))

    expected_metadata_list = [
        {
            "total_docs_count": 1,
        },  # transform() metadata
        {},  # Empty flush() metadata
    ]


if __name__ == "__main__":
    t = TestDocQualityTransform()
