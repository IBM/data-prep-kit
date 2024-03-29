from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from lang_id_transform import (
    PARAM_CONTENT_COLUMN_NAME,
    PARAM_MODEL_CREDENTIAL,
    PARAM_MODEL_KIND,
    PARAM_MODEL_URL,
    LangIdentificationTransform,
)
from lang_models import KIND_FASTTEXT


class TestLangIdentificationTransform(AbstractTransformTest):
    def get_test_transform_fixtures(self) -> list[Tuple]:
        config = {
            PARAM_MODEL_KIND: KIND_FASTTEXT,
            PARAM_MODEL_CREDENTIAL: "YOUR HUGGING FACE ACCOUNT TOKEN",
            PARAM_MODEL_URL: "facebook/fasttext-language-identification",
            PARAM_CONTENT_COLUMN_NAME: "contents",
        }
        table = pa.Table.from_arrays(
            [
                pa.array(
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
            ],
            names=["contents"],
        )
        expected_table = pa.Table.from_arrays(
            [
                pa.array(["de", "pt", "ja", "fr", "es"]),
                pa.array(
                    [
                        0.998,
                        1.000,
                        0.930,
                        0.998,
                        0.998,
                    ]
                ),
            ],
            names=["ft_lang", "ft_score"],
        )
        return [
            (
                LangIdentificationTransform(config),
                [table],
                [expected_table],
                [{"de": 1, "es": 1, "fr": 1, "ja": 1, "pt": 1}, {}],
            )
        ]
