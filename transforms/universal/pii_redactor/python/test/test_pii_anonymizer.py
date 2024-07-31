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
    results = analyzer.analyze_text(sample_input)
    anonymized_results = anonymizer.anonymize_text(sample_input, results)
    assert anonymized_results.text != sample_input


def test_input_not_modified_for_non_pii_text(analyzer, anonymizer):
    sample_input = "Hello world. Exploring new tech stack is really cool !!"
    results = analyzer.analyze_text(sample_input)
    anonymized_results = anonymizer.anonymize_text(sample_input, results)
    assert anonymized_results.text == sample_input
