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
        "This is a sample test which has my name Sowmya and my email as sowmya@techiediver.com and " "self.config"
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
