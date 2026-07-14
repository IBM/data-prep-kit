# SPDX-License-Identifier: Apache-2.0
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

import logging
from unittest.mock import patch

import pyarrow as pa
from dpk_gneissweb_classification.transform import ClassificationTransform


def _make_transform(model_urls, label_columns, score_columns):
    """Build a ClassificationTransform without going through __init__,
    which would try to download real fastText models over the network.
    Only the attributes transform() actually reads are set."""
    t = ClassificationTransform.__new__(ClassificationTransform)
    t.model = list(model_urls)
    t.model_url = list(model_urls)
    t.content_column_name = "text"
    t.output_label_column_name = list(label_columns)
    t.output_score_column_name = list(score_columns)
    t.n_processes = 1
    t.logger = logging.getLogger("test")
    return t


def test_transform_merges_stats_across_multiple_classifiers():
    """Regression test for #1453: stats from earlier classifiers must not
    be overwritten by later ones when multiple classifiers are configured."""
    t = _make_transform(
        model_urls=["url_a", "url_b"],
        label_columns=["label_a", "label_b"],
        score_columns=["score_a", "score_b"],
    )
    table = pa.table({"text": ["doc1", "doc2"]})

    fake_results = [
        (table, {"topic_a_label": 2}),
        (table, {"topic_b_label": 5}),
    ]

    with patch(
        "dpk_gneissweb_classification.transform.get_label_ds_pa",
        side_effect=fake_results,
    ):
        _, stats = t.transform(table)

    assert stats == {"topic_a_label": 2, "topic_b_label": 5}


def test_transform_single_classifier_unaffected():
    """The single-classifier case (what the existing integration test
    already covers) must produce the same result before and after the fix."""
    t = _make_transform(
        model_urls=["url_med"],
        label_columns=["label_med"],
        score_columns=["score"],
    )
    table = pa.table({"text": ["doc1"]})

    fake_results = [(table, {"medical": 1})]

    with patch(
        "dpk_gneissweb_classification.transform.get_label_ds_pa",
        side_effect=fake_results,
    ):
        _, stats = t.transform(table)

    assert stats == {"medical": 1}
