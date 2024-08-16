import logging

import spacy
from flair_recognizer import FlairRecognizer
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider


# Initialize logger
log = logging.getLogger(__name__)

# Define constants
NLP_ENGINE = "flair/ner-english-large"
SPACY_MODEL = "en_core_web_sm"


class PIIAnalyzerEngine:
    def __init__(self, supported_entities=None, score_threshold=None):
        self.supported_entities = supported_entities
        self.score_threshold = score_threshold
        self.nlp_engine, self.registry = self._create_nlp_engine()

    def _create_nlp_engine(self):
        """
        Creates and configures the NLP engine and recognizer registry for PII analysis.

        Returns:
            nlp_engine (NlpEngineProvider): Configured NLP engine.
            registry (RecognizerRegistry): Configured recognizer registry with Flair recognizer.
        """
        registry = RecognizerRegistry()
        registry.load_predefined_recognizers()

        # Ensure the required Spacy model is available
        if not spacy.util.is_package(SPACY_MODEL):
            log.info(f"========= downloading {SPACY_MODEL} ==========")
            spacy.cli.download(SPACY_MODEL)

        # Add Flair recognizer to the registry
        flair_recognizer = FlairRecognizer(model_path=NLP_ENGINE, supported_entities=self.supported_entities)
        registry.add_recognizer(flair_recognizer)

        # Remove the default Spacy recognizer
        registry.remove_recognizer("SpacyRecognizer")

        # Configure the NLP engine with Spacy
        nlp_configuration = {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": SPACY_MODEL}],
        }
        nlp_engine = NlpEngineProvider(nlp_configuration=nlp_configuration).create_engine()

        return nlp_engine, registry

    def analyze_text(self, text, language="en"):
        """
        Analyzes the given text to identify PII entities.

        Args:
            text (str): The text to analyze.
            language (str): The language of the text (default is "en").

        Returns:
            List[RecognizerResult]: Results of the PII analysis.
            List[entity_types]: Types of PII entities identified in the given input text
        """
        analyzer = AnalyzerEngine(nlp_engine=self.nlp_engine, registry=self.registry)
        analyze_results = analyzer.analyze(text=text, language=language, entities=self.supported_entities,
                                           score_threshold=self.score_threshold)
        entity_types = [result.entity_type for result in analyze_results]
        return analyze_results, entity_types
