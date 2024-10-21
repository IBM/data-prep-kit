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

"""
Referred from
https://github.com/sahajsoft/pii-detection-and-anonymizer/blob/main/src/recognizer/flair_recognizer.py

"""
from typing import List, Optional, Set, Tuple

from data_processing.utils import get_logger
from flair.data import Sentence
from flair.models import SequenceTagger
from presidio_analyzer import AnalysisExplanation, EntityRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts


log = get_logger(__name__)


class FlairRecognizer(EntityRecognizer):
    ENTITIES = [
        "LOCATION",
        "PERSON",
        "ORGANIZATION",
    ]

    DEFAULT_EXPLANATION = "Identified as {} by Flair's Named Entity Recognition"

    CHECK_LABEL_GROUPS = [
        ({"LOCATION"}, {"LOC", "LOCATION"}),
        ({"PERSON"}, {"PER", "PERSON"}),
        ({"ORGANIZATION"}, {"ORG"}),
    ]

    MODEL_LANGUAGES = {"en": "flair/ner-english-large"}

    PRESIDIO_EQUIVALENCES = {
        "PER": "PERSON",
        "LOC": "LOCATION",
        "ORG": "ORGANIZATION",
    }

    def __init__(
        self,
        supported_language: str = "en",
        supported_entities: Optional[List[str]] = None,
        check_label_groups: Optional[Tuple[Set, Set]] = None,
        model: SequenceTagger = None,
        model_path: Optional[str] = None,
    ):
        self.check_label_groups = check_label_groups if check_label_groups else self.CHECK_LABEL_GROUPS

        supported_entities = supported_entities if supported_entities else self.ENTITIES

        if model and model_path:
            raise ValueError("Only one of model or model_path should be provided.")
        elif model and not model_path:
            self.model = model
        elif not model and model_path:
            log.info(f"Loading model from {model_path}")
            self.model = SequenceTagger.load(model_path)
        else:
            log.info(f"Loading model for language {supported_language}")
            self.model = SequenceTagger.load(self.MODEL_LANGUAGES.get(supported_language))

        super().__init__(
            supported_entities=supported_entities,
            supported_language=supported_language,
            name="Flair Analytics",
        )

    def load(self) -> None:
        """Load the model, not used. Model is loaded during initialization."""
        pass

    def get_supported_entities(self) -> List[str]:
        """
        Return supported entities by this model.
        :return: List of the supported entities.
        """
        return self.supported_entities

    # Class to use Flair with Presidio as an external recognizer.
    def analyze(
        self, text: str, entities: List[str] = None, nlp_artifacts: NlpArtifacts = None
    ) -> List[RecognizerResult]:
        """
        Analyze text using Text Analytics.
        :param text: The text for analysis.
        :param entities: pii entities.
        :param nlp_artifacts: Not used by this recognizer.
        :param language: Text language. Supported languages in MODEL_LANGUAGES
        :return: The list of Presidio RecognizerResult constructed from the recognized
            Flair detections.
        """

        results = []

        sentences = Sentence(text)
        self.model.predict(sentences)

        # If there are no specific list of entities, we will look for all of it.
        if not entities:
            entities = self.supported_entities

        for entity in entities:
            if entity not in self.supported_entities:
                continue

            for ent in sentences.get_spans("ner"):
                if not self.__check_label(entity, ent.labels[0].value, self.check_label_groups):
                    continue
                textual_explanation = self.DEFAULT_EXPLANATION.format(ent.labels[0].value)
                explanation = self.build_flair_explanation(round(ent.score, 2), textual_explanation)
                flair_result = self._convert_to_recognizer_result(ent, explanation)

                results.append(flair_result)

        return results

    def _convert_to_recognizer_result(self, entity, explanation) -> RecognizerResult:
        entity_type = self.PRESIDIO_EQUIVALENCES.get(entity.tag, entity.tag)
        flair_score = round(entity.score, 2)

        flair_results = RecognizerResult(
            entity_type=entity_type,
            start=entity.start_position,
            end=entity.end_position,
            score=flair_score,
            analysis_explanation=explanation,
        )

        return flair_results

    def build_flair_explanation(self, original_score: float, explanation: str) -> AnalysisExplanation:
        """
        Create explanation for why this result was detected.
        :param original_score: Score given by this recognizer
        :param explanation: Explanation string
        :return:
        """
        explanation = AnalysisExplanation(
            recognizer=self.__class__.__name__,
            original_score=original_score,
            textual_explanation=explanation,
        )
        return explanation

    @staticmethod
    def __check_label(entity: str, label: str, check_label_groups: Tuple[Set, Set]) -> bool:
        return any([entity in egrp and label in lgrp for egrp, lgrp in check_label_groups])
