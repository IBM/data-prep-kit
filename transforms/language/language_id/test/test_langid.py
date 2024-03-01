import pyarrow as pa
from lang_models import KIND_FASTTEXT, LangModel, LangModelFactory
from watson_nlp import get_lang_ds_pa


def test_language_identification():
    nlp_langid = LangModelFactory.create_model(KIND_FASTTEXT, "/root/lid.176.ftz")
    print("3")
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
    print("4")
    table = pa.Table.from_arrays([documents], names=["contents"])
    print("5")
    table, stats = get_lang_ds_pa(table, nlp_langid, col_name="contents")
    print("6")
    print(stats)
    print("7")
    assert table["ft_lang"].to_pylist() == ["de", "pt", "ja", "fr", "es"]
