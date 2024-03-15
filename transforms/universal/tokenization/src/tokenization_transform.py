"""
Libraries need to be added to venv:
    transformers       4.35.0
"""

import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import get_logger
import os,sys

logger = get_logger(__name__)

local_tokenizer = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tokenizers", "gpt2_based"))

from transformers import AutoTokenizer, PreTrainedTokenizerBase, PreTrainedTokenizerFast


class TokenizationTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, TokenizationTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of TokenizationTransformConfiguration class

        super().__init__(config)
        self.tokenizer_path = config.get("tokenizer_path", local_tokenizer)
        self.doc_id_column = config.get("doc_id_column", "document_id")
        self.doc_content_column = config.get("doc_content_column", "contents")

        self.sleep = config.get("sleep", 1)

        print(f"\n*** `config` to run:")
        for k,v in config.items():
            print(f"{k:20s}: {v}")

        # TODO load tokenizer here or in transform()?:
        self.tokenizer = self._load_tokenizer()


    def _load_tokenizer(self):
        """
        Load and return a tokenizer
        TODO:
            + supporting **kwargs in config to load HF tokenizer:
                tokenizer = AutoTokenizer.from_pretrained(tokenizer_type, **kwargs)
            + download pre-trained tokenizer from COS?
            + put tokenizer to ray object storage?
        """
        try:
            tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_path)
        except Exception as e:
            sys.exit(f"Failed to load tokenizer from {self.tokenizer_path} due to\n: {e}")

        # quick test tokenizer:
        txt = "This text is for testing"
        token_line = tokenizer(txt)["input_ids"]
        print(f"== Quick test tokenizer by tokenizing `{txt}` to: {token_line}")

        return tokenizer

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        TODO:
            + Need specific columns only ds = ray.data.read_parquet(pq_file, filesystem=s3, columns=columns)
            + support column mapping to [document_id, contents] (should rename it as content? or keep contents in DP?)
        """
        logger.debug(f"Transforming one table with {len(table)} rows using tokenizer {self.tokenizer_path}")


        # Token counts from tokenized doc, eg: [(`doc1`,200), (`doc2`,400)]
        token_counts = []

        # To store doc_ids of empty rows/docs:
        empty_doc_ids = []
        # To store token sequences for output (arrow) table, eg: [[978, 1923, 313, 317], [317, 4294]]
        doc_tokens = []

        for idx in range(table.num_rows):
            doc_id = table[self.doc_id_column][idx].as_py()
            doc_content = table[self.doc_content_column][idx].as_py()
            try:
                token_line = self.tokenizer(doc_content)["input_ids"]
            except Exception as e:
                # skip failed row/doc, treat it as `empty` and move on:
                logger.info(f"Failed in tokenizing `{doc_content}` due to:\n {e}")
                empty_doc_ids.append(doc_id)
                continue

            num_tokens = len(token_line)

            # skip empty document:
            if num_tokens == 0:
                empty_doc_ids.append(doc_id)
                continue
            else:
                doc_tokens.append(token_line)
                token_counts.append([doc_id, num_tokens])

        out_table = pa.table({"tokens": doc_tokens})
        logger.debug(f"Done with the transformed table with {table.num_rows:,} rows")
        metadata = {"num_rows": f"{table.num_rows:,}",
                    "num_tokenized_rows": f"{out_table.num_rows:,}",
                    "num_empty/failed_rows": f"{len(empty_doc_ids):,}",
                    "token_count_per_doc": token_counts,
                    }

        return [out_table], metadata


class TokenizationTransformConfiguration(DefaultTableTransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="Tokenization", transform_class=TokenizationTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the TokenizationTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, tkn_, pii_, etc.)
        """
        parser.add_argument(
            "--tkn_tokenizer_path",
            type=str,
            default=local_tokenizer,
            help="Path to tokenizer used for tokenization. It can be a path to a local tokenizer or a downloadable HuggingFace tokenizer",
        )
        parser.add_argument(
            "--tkn_doc_id_column",
            type=str,
            default='document_id',
            help="Column contains document id which values should be unique across dataset",
        )

        parser.add_argument(
            "--tkn_doc_content_column",
            type=str,
            default='contents',
            help="Column contains document content",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.tkn_tokenizer_path is None:
            print(f"Parameter tokenizer_path must be a valid tokenizer for tokenization, you specified {args.tokenizer_path}")
            return False

        if args.tkn_doc_id_column is None or args.tkn_doc_content_column is None:
            print(f"Parameter for `tkn_doc_id_column` and `tkn_doc_content_column` must be provided")
            return False

        self.params["tokenizer_path"] = args.tkn_tokenizer_path
        self.params["doc_id_column"] = args.tkn_doc_id_column
        self.params["doc_content_column"] = args.tkn_doc_content_column

        return True


if __name__ == "__main__":
    config = {"tokenizer_path":local_tokenizer,
              "doc_id_column":"document_id",
              "doc_content_column":"contents",
              "sleep": 0,
              }
    tkn = TokenizationTransform(config)

    in_table = pa.Table.from_pydict({"document_id": pa.array(["doc01","doc02","doc03"]),
                                     "contents": pa.array(["Content for doc01","","doc03 contents"])})

    out_table, metadata = tkn.transform(in_table)
    print(f"\n== out_table: {out_table}")
    print(f"\n== metadata: {metadata}")
    # launcher = TransformLauncher(transform_runtime_config=TokenizationTransformConfiguration())
    # logger.info("Launching Tokenization transform")
    # launcher.launch()
