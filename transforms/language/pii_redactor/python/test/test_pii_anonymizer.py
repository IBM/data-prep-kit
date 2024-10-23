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

import pytest
from pii_analyzer import PIIAnalyzerEngine
from pii_anonymizer import PIIAnonymizer


@pytest.fixture(scope="module")
def analyzer():
    """
    Fixture to initialize PIIAnalyzerEngine once per module.
    """
    return PIIAnalyzerEngine()


@pytest.fixture(scope="module")
def anonymizer():
    """
    Fixture to initialize PIIAnonymizer once per module.
    """
    return PIIAnonymizer()


def test_pii_anonymised_text_for_pii_data(analyzer, anonymizer):
    sample_input = "This sample text has name Sowmya and email id as sowmya@gmail.com"
    results, _ = analyzer.analyze_text(sample_input)
    anonymized_results = anonymizer.anonymize_text(sample_input, results)
    assert anonymized_results.text != sample_input
    assert "Sowmya" not in anonymized_results.text
    assert "sowmya@gmail.com" not in anonymized_results.text


def test_input_not_modified_for_non_pii_text(analyzer, anonymizer):
    sample_input = "Hello world. Exploring new tech stack is really cool !!"
    results, _ = analyzer.analyze_text(sample_input)
    anonymized_results = anonymizer.anonymize_text(sample_input, results)
    assert anonymized_results.text == sample_input
