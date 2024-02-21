import os


os.environ["OPENBLAS_NUM_THREADS"] = "1"

from typing import List, Tuple

import pyarrow
import pyarrow as pa
import pyizumo


# On ROKS Cluster
runtime_env = {
    "env_vars": {
        "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
        "JVM_PATH": "/usr/lib/jvm/java-11-openjdk-amd64/lib/server/libjvm.so",
    }
}


def get_score(lang: str) -> float:
    # Note: pyizumo does not return confidence score. Set to 1.0 as default.
    # Other libraries may return confidence score.
    return 1.0


def get_lang_ds_pa(
    table: pyarrow.table, nlp: pyizumo.model.Izumo, col_name: str = "contents"
) -> tuple[pyarrow.table, pyarrow.table]:
    try:
        detected_language = pa.array(list(map(lambda x: nlp(x).locale, table[col_name].to_pylist())))
    except Exception as e:
        print("ERROR:", e, "skipping the file")
        return None, None
    stats = pa.table([detected_language], names=["lang"]).group_by("lang").aggregate([("lang", "count")]).to_pandas()
    table = table.append_column("ft_lang", detected_language)
    table = table.append_column("ft_score", pa.array(list(map(lambda x: get_score(x), table["ft_lang"].to_pylist()))))
    return table, stats


def get_sentences_ds_pa(
    table: pyarrow.table, ft_lang: str, nlp: pyizumo.model.Izumo, col_name: str = "contents"
) -> list[pyarrow.table]:
    """
    Converts a (batch) dataset where each record (row) is a document to a dataset
    where each record (row) is a sentence.
    """
    sentence_dicts = []
    sentence_counts = 0
    data = table.to_pydict()
    for i in range(table.num_rows):
        # keep all the other attributes of the row in dict
        d = {k: v[i] for k, v in data.items()}
        content = d.pop(col_name)
        if ft_lang == "en":
            sentences = [str(x) for x in list(nlp(content).sentences)]
        elif ft_lang == "ja":
            # Japanese lang
            sentences = list(map(str.strip, content.split("。")))
        else:
            # other non-English languages
            sentences = list(map(str.strip, content.split(".")))

        document_id = d.pop("document_id")
        for i, sentence in enumerate(sentences):
            if len(sentence) > 1:
                sentence_dict = {**d, "document_id": document_id, "sentence_id": i, "sentence_text": sentence[:500]}
                sentence_dicts.append(sentence_dict)
                sentence_counts += len(sentence_dict)

    # print(f"total number of sentences: {sentence_counts}")
    new_tables = []
    result = {}
    cnt = 0
    for d in sentence_dicts:
        for k, v in d.items():
            result.setdefault(k, []).append(v)
        cnt += 1
        if cnt > 1000000:
            new_tables.append(pa.Table.from_pydict(result))
            result = {}
            cnt = 0
    if len(result) > 0:
        new_tables.append(pa.Table.from_pydict(result))
    return new_tables
