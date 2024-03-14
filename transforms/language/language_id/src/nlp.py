import os


os.environ["OPENBLAS_NUM_THREADS"] = "1"

from typing import Any

import pandas as pd
import pyarrow as pa
from lang_models import LangModel


def get_lang_ds_pa(table: pa.table, nlp: LangModel, col_name: str = "contents") -> tuple[pa.table, dict[str, Any]]:
    try:
        detected_language = pa.table(
            pd.DataFrame(map(lambda x: nlp.detect_lang(x), table[col_name].to_pylist()), columns=["lang", "score"])
        )
    except Exception as e:
        print("ERROR:", e, "skipping the file")
        return None, None
    stats = pa.table([detected_language["lang"]], names=["lang"]).group_by("lang").aggregate([("lang", "count")])
    stats_dict = {}
    for batch in stats.to_batches():
        d = batch.to_pydict()
        for lang, count in zip(d["lang"], d["lang_count"]):
            stats_dict[lang] = count
    result = pa.table([detected_language["lang"]], names=["ft_lang"])
    result = result.append_column("ft_score", detected_language["score"])
    return result, stats_dict
