import pytest
from pii_analyzer import PIIAnalyzerEngine


@pytest.fixture(scope="module")
def analyzer():
    """
    Fixture to initialize PIIAnalyzerEngine once per module.
    """
    return PIIAnalyzerEngine(supported_entities=["PERSON", "EMAIL_ADDRESS"])


def test_analyse_text_for_pii_data(analyzer):
    input_text = (
        "This is a sample test which has my name Sowmya.L.R and my email as sowmya@techiediver.com and " "self.config"
    )
    result = analyzer.analyze_text(input_text, language="en")
    assert result


def test_analyse_results_of_non_pii_text_data(analyzer):
    input_text = "This is a sample test"
    result = analyzer.analyze_text(input_text, language="en")
    assert len(result) == 0


def test_analyse_for_empty_blank_input(analyzer):
    input_text = ""
    result = analyzer.analyze_text(input_text, language="en")
    assert len(result) == 0
    input_text = " "
    result = analyzer.analyze_text(input_text, language="en")
    assert len(result) == 0
