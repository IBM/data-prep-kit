import pyarrow as pa
from lang_models import KIND_FASTTEXT, LangModel, LangModelFactory
from nlp import get_lang_ds_pa


def test_language_identification():
    nlp_langid = LangModelFactory.create_model(
        KIND_FASTTEXT, "facebook/fasttext-language-identification", "YOUR HUGGING FACE ACCOUNT TOKEN"
    )
    documents = pa.array(
        [
            "Der Tell Sabi Abyad („Hügel des weißen Jungen“) ist eine historische "
            "Siedlungsstätte im Belich-Tal in Nordsyrien nahe bei dem modernen Dorf Hammam et-Turkman, ",
            "Mu2 Gruis (36 Gruis) é uma estrela na direção da constelação de Grus. Possui uma ascensão "
            "reta de 22h 16m 26.57s e uma declinação de −41° 37′ 37.9″. Sua magnitude aparente é igual a 5.11. ",
            "タッチダウンオフィスは、LANと電源を用意して他のオフィスからやってきた利用者が作業できる環境を整えた場所。 " "生産性の向上を目的に通常オフィスの内部に設けられる。",
            "Raneem El Weleily, née le à Alexandrie, est une joueuse professionnelle de squash représentant l'Égypte. "
            "Elle atteint, en septembre 2015, la première place mondiale sur le circuit international, ",
            "En la mitología griega, Amarinceo (en griego Ἀμαρυγκεύς) era un caudillo de los eleos. "
            "Su padre es llamado Aléctor o Acetor o bien un tal Onesímaco,nombre dudoso. Su madre era Diogenía, "
            "hija de Forbante y nieta de Lápites. ",
        ]
    )
    table = pa.Table.from_arrays([documents], names=["contents"])
    table, stats = get_lang_ds_pa(table, nlp_langid, col_name="contents")
    assert table["ft_lang"].to_pylist() == ["de", "pt", "ja", "fr", "es"]


# def test_sentence_split(nlp_sentence: pyizumo.model.Izumo):
#    nlp_sentence = pyizumo.load("ja", parsers=["sentence"])
#    document_ids = pa.array([1001])
#    documents = pa.array(
#        [
#            "タッチダウンオフィスは、LANと電源を用意して他のオフィスからやってきた利用者が作業できる環境を整えた場所。 "
#            "生産性の向上を目的に通常オフィスの内部に設けられる。 東京駅前に企業と契約して社員にタッチダウンオフィスを提供す"
#            "るサービスを展開するデスカットのような施設もある。 余ったスペースにLANと電源を用意してつくった出張者席。 "
#            "社内のバッファスペースとして位置付けていたが、オフィス面積の削減で営業社員に固定席を設定しないフリーアドレス制を導入したために、"
#            "全部の席がタッチダウン化してしまう事もある。 たった2時間の会議の為に出張したものの、気になるのでメールだけでもずっとみていたい、"
#            "という時には便利である。"
#        ]
#    )
#    table = pa.Table.from_arrays([document_ids, documents], names=["document_id", "contents"])

#    tables = get_sentences_ds_pa(table, "ja", nlp_sentence, col_name="contents")
#    assert tables[0].shape[0] == 6
