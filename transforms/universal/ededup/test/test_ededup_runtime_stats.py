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

from unittest.mock import MagicMock

import pytest
from data_processing.transform import TransformStatistics
from dpk_ededup.runtime import EdedupRuntime
from dpk_ededup.transform_base import HashFilter


def _make_runtime() -> EdedupRuntime:
    runtime = EdedupRuntime({})
    # get_transform_config() normally sets this up; construct a fresh, empty filter directly
    # since these tests exercise compute_execution_stats()'s stats computation in isolation.
    # snapshot() needs a real data-access backend to persist to, which is unrelated to what's
    # under test here, so it's mocked out.
    runtime.filter = HashFilter({})
    runtime.filter.snapshot = MagicMock()
    return runtime


def test_compute_execution_stats_with_zero_source_documents_does_not_raise():
    """
    Regression test: a run over an empty input set (source_documents == 0) must not raise
    ZeroDivisionError. dict.get(key, default) only applies the default when the key is absent,
    not when it is present with value 0, so a naive `stats.get("source_documents", 1)` guard
    does not actually protect against this case.
    """
    runtime = _make_runtime()
    stats = TransformStatistics()
    stats.add_stats({"source_documents": 0, "result_documents": 0})

    runtime.compute_execution_stats(stats)

    result = stats.get_execution_stats()
    assert result["de duplication %"] == 0.0


def test_compute_execution_stats_normal_case_unchanged():
    runtime = _make_runtime()
    stats = TransformStatistics()
    stats.add_stats({"source_documents": 10, "result_documents": 7})

    runtime.compute_execution_stats(stats)

    result = stats.get_execution_stats()
    assert result["de duplication %"] == pytest.approx(30.0)
