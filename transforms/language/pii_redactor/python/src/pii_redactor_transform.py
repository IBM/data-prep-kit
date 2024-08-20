"""
Author: Sowmya.L.R, email:lrsowmya@gmail.com
"""

import argparse
import ast
import json
from typing import Any, Optional

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, get_logger
from pii_analyzer import PIIAnalyzerEngine
from pii_anonymizer import PIIAnonymizer


short_name = "pii_redactor"
cli_prefix = short_name + "_"
supported_entities_key = "entities"
redaction_operator_key = "operator"
doc_transformed_contents_key = "transformed_contents"
score_threshold_key = "score_threshold"
pii_contents_column = "contents"


supported_entities_cli_param = f"{cli_prefix}{supported_entities_key}"
redaction_operator_cli_param = f"{cli_prefix}{redaction_operator_key}"
doc_transformed_contents_cli_param = f"{cli_prefix}{doc_transformed_contents_key}"
score_threshold_cli_param = f"{cli_prefix}{score_threshold_key}"

default_score_threshold_key = 0.6
"""
The default score_threshold value will be 0.6. At this range false positives are reduced significantly
"""

default_supported_entities = ["PERSON", "EMAIL_ADDRESS", "ORGANIZATION", "DATE_TIME", "CREDIT_CARD", "PHONE_NUMBER"]
"""By default it supports person name, email address, organization, date, credti card, phone number.To know more about entities refer https://microsoft.github.io/presidio/supported_entities/
"""

default_anonymizer_operator = "replace"
"""By default the anonymizer operator is replace which will replace it with the respective entity"""


class PIIRedactorTransform(AbstractTableTransform):
    """
    Implements PII redactor transform.
    needed contents column name, anonymizer operator replace|redact
    """

    def __init__(self, config: dict):
        super().__init__(config)

        self.log = get_logger(__name__)
        self.supported_entities = config.get(supported_entities_key, default_supported_entities)
        self.redaction_operator = config.get(redaction_operator_key, default_anonymizer_operator)
        self.doc_contents_key = config.get(doc_transformed_contents_key)
        score_threshold_value = config.get(score_threshold_key, default_score_threshold_key)

        self.analyzer = PIIAnalyzerEngine(
            supported_entities=self.supported_entities, score_threshold=score_threshold_value
        )
        self.anonymizer = PIIAnonymizer(operator=self.redaction_operator.lower())

    def _analyze_pii(self, text: str):
        return self.analyzer.analyze_text(text)

    def _redact_pii(self, text: str):
        text = text.strip()
        if text:
            analyze_results, entity_types = self._analyze_pii(text)
            anonymized_results = self.anonymizer.anonymize_text(text, analyze_results)
            return anonymized_results.text, entity_types

    def transform(self, table: pa.Table, file_name: Optional[str] = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Apply  transformation to contents column in the input table for redacting pii contents.

        :param table: The input Arrow table to transform.
        :param file_name: Optional file name (not used in this implementation).
        :return: A new Arrow table with the transformed column.
        """

        TransformUtils.validate_columns(table=table, required=[pii_contents_column])
        metadata = {"original_table_rows": table.num_rows, "original_column_count": len(table.column_names)}


        redacted_texts, entity_types_list = zip(*table[pii_contents_column].to_pandas().apply(self._redact_pii))
        table = table.add_column(0, self.doc_contents_key, [redacted_texts])
        table = table.add_column(0, "detected_pii", [entity_types_list])
        metadata["transformed_table_rows"] = table.num_rows
        metadata["transformed_column_count"] = len(table.column_names)

        return [table], metadata


class PIIRedactorTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=PIIRedactorTransform,
        )

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            f"--{supported_entities_cli_param}",
            type=ast.literal_eval,
            required=False,
            default=ast.literal_eval("[]"),
            help=f"List of entities to be redacted from the input data: {json.dumps(default_supported_entities, indent=2, default=str)}. "
            f"To know more about entities refer https://microsoft.github.io/presidio/supported_entities/",
        )
        parser.add_argument(
            f"--{redaction_operator_cli_param}",
            type=str,
            required=False,
            default="replace",
            help="Redaction technique to be applied on detected pii data.Supported techniques redact, replace",
        )

        parser.add_argument(
            f"--{doc_transformed_contents_cli_param}",
            type=str,
            required=True,
            default="new_contents",
            help="Mention column name in which transformed contents will be added",
        )

        parser.add_argument(
            f"--{score_threshold_cli_param}",
            type=float,
            required=False,
            default=0.6,
            help="The score_threshold is a parameter that "
            "sets the minimum confidence score required for an entity to be considered a match."
            "Provide a value above 0.6",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True
