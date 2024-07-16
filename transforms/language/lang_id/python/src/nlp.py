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

from typing import Any

import pyarrow as pa
from data_processing.utils import TransformUtils, get_logger
from lang_models import LangModel


logger = get_logger(__name__)


def get_lang_ds_pa(table: pa.table, nlp: LangModel, col_name: str = "contents") -> tuple[pa.table, dict[str, Any]]:
    detected_language = pa.Table.from_pylist(
        list(
            map(
                lambda r: {"lang": r[0], "score": r[1]},
                map(lambda x: nlp.detect_lang(x), table[col_name].to_pylist()),
            )
        )
    )
    stats = pa.table([detected_language["lang"]], names=["lang"]).group_by("lang").aggregate([("lang", "count")])
    stats_dict = {}
    for batch in stats.to_batches():
        d = batch.to_pydict()
        for lang, count in zip(d["lang"], d["lang_count"]):
            stats_dict[lang] = count
    result = TransformUtils.add_column(table=table, name="ft_lang", content=detected_language["lang"])
    result = TransformUtils.add_column(table=result, name="ft_score", content=detected_language["score"])
    return result, stats_dict
