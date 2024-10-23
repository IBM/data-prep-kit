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


@pytest.fixture(scope="module")
def analyzer():
    """
    Fixture to initialize PIIAnalyzerEngine once per module.
    """
    supported_entities = ["PERSON", "EMAIL_ADDRESS", "DATE_TIME", "URL", "CREDIT_CARD", "PHONE_NUMBER", "LOCATION"]
    score_threshold = 0.6
    return PIIAnalyzerEngine(supported_entities=supported_entities, score_threshold=score_threshold)


def test_analyse_text_for_pii_data(analyzer):
    input_text = (
        "This is a sample test which has my name Sowmya and my email as sowmya@techiediver.com and "
        "self.config"
        "Born on 31.05.2021"
    )
    result, entity_types = analyzer.analyze_text(input_text, language="en")
    assert result
    assert len(entity_types) > 0


def test_analyse_results_of_non_pii_text_data(analyzer):
    input_text = "This is a sample test"
    result, entity_types = analyzer.analyze_text(input_text, language="en")
    assert len(result) == 0
    assert len(entity_types) == 0


def test_analyse_for_empty_blank_input(analyzer):
    input_text = ""
    result, entity_types = analyzer.analyze_text(input_text, language="en")
    assert len(result) == 0
    assert len(entity_types) == 0
    input_text = " "
    result, entity_types = analyzer.analyze_text(input_text, language="en")
    assert len(result) == 0
    assert len(entity_types) == 0


def test_analyse_credit_card_number_correctly(analyzer):
    input_text = "Hello, my name is John Davidson and I live in Florida.My credit card number is 4095-2609-9393-4932"
    result, entity_types = analyzer.analyze_text(input_text, language="en")
    assert len(entity_types) == 3
    assert "CREDIT_CARD" in entity_types
    assert result
