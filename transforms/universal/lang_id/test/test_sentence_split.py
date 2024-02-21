import pyarrow as pa
import pyizumo

from transforms.universal.lang_id.watson_nlp import get_sentences_ds_pa


def test_sentence_split():
    document_ids = pa.array([1001])
    documents = pa.array(
        [
            "タッチダウンオフィスは、LANと電源を用意して他のオフィスからやってきた利用者が作業できる環境を整えた場所。 "
            "生産性の向上を目的に通常オフィスの内部に設けられる。 東京駅前に企業と契約して社員にタッチダウンオフィスを提供す"
            "るサービスを展開するデスカットのような施設もある。 余ったスペースにLANと電源を用意してつくった出張者席。 "
            "社内のバッファスペースとして位置付けていたが、オフィス面積の削減で営業社員に固定席を設定しないフリーアドレス制を導入したために、"
            "全部の席がタッチダウン化してしまう事もある。 たった2時間の会議の為に出張したものの、気になるのでメールだけでもずっとみていたい、"
            "という時には便利である。"
        ]
    )
    table = pa.Table.from_arrays([document_ids, documents], names=["document_id", "contents"])
    nlp_sentence = pyizumo.load("ja", parsers=["sentence"])
    tables = get_sentences_ds_pa(table, "ja", nlp_sentence, col_name="contents")
    assert tables[0].shape[0] == 6


if __name__ == "__main__":
    test_sentence_split()
