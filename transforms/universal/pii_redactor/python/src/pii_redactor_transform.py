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
anonymizer_operator_key = "operator"
doc_contents_key = "contents"

supported_entities_cli_param = f"{cli_prefix}{supported_entities_key}"
anonymizer_operator_cli_param = f"{cli_prefix}{anonymizer_operator_key}"
doc_contents_cli_param = f"{cli_prefix}{doc_contents_key}"

default_supported_entities = ["PERSON", "EMAIL_ADDRESS", "ORGANIZATION", "DATE_TIME"]
"""By default it supports person name, email address.To know more about entities refer https://microsoft.github.io/presidio/supported_entities/
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
        self.anonymizer_operator = config.get(anonymizer_operator_key, default_anonymizer_operator)
        self.doc_contents_key = config.get(doc_contents_key, doc_contents_key)
        self.analyzer = PIIAnalyzerEngine(supported_entities=self.supported_entities)
        self.anonymizer = PIIAnonymizer(operator=self.anonymizer_operator)

    def _analyze_pii(self, text: str):
        return self.analyzer.analyze_text(text)

    def _redact_pii(self, text: str):
        text = text.strip()
        if text:
            analyze_results = self._analyze_pii(text)
            anonymized_results = self.anonymizer.anonymize_text(text, analyze_results)
            return anonymized_results.text

    def transform(self, table: pa.Table, file_name: Optional[str] = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Apply  transformation to contents column in the input table for redacting pii contents.

        :param table: The input Arrow table to transform.
        :param file_name: Optional file name (not used in this implementation).
        :return: A new Arrow table with the transformed column.
        """

        TransformUtils.validate_columns(table=table, required=[self.doc_contents_key])
        metadata = {"original_table_rows": table.num_rows, "original_column_count": len(table.column_names)}

        # contents_data = table[self.doc_contents_key]
        # transformed_data = pa.array([self._redact_pii(text) for text in contents_data])
        # redacted_data = [
        #     transformed_data if name == self.doc_contents_key else table[name] for name in table.column_names
        # ]
        # transformed_table = pa.Table.from_arrays(redacted_data, table.schema)
        # metadata["transformed_table_rows"] = transformed_table.num_rows

        for column in table.column_names:

            if column == self.doc_contents_key:
                column_data = pa.array(table[column].to_pandas().apply(self._redact_pii))
                table = table.add_column(0, "new_contents", [column_data])
        metadata["transformed_table_rows"] = table.num_rows
        metadata["transformed_column_count"] = len(table.column_names)
        print(f"transformed table=====\n {table}")
        # print(table)
        print(f"metadata=============\n {metadata}")
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
            help=f"list of entities to be redacted from the input data: {json.dumps(default_supported_entities, indent=2, default=str)}. "
            f"To know more about entities refer https://microsoft.github.io/presidio/supported_entities/",
        )
        parser.add_argument(
            f"--{anonymizer_operator_cli_param}",
            type=str,
            required=False,
            default="replace",
            help="anonymization technique to be applied on detected pii data.Supported techniques redact, replace",
        )

        parser.add_argument(
            f"--{doc_contents_cli_param}",
            type=str,
            required=True,
            default="contents",
            help="mention the column name which needs to be transformed for pii",
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
