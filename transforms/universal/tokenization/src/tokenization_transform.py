"""
Libraries need to be added to venv:
    transformers==4.35.0
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
logger = get_logger(__name__)


from tokenization_utils import split_text, load_tokenizer

class TokenizationTransform(AbstractTableTransform):
    """
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
        self.tokenizer = config.get("tokenizer", "bigcode/starcoder")
        self.doc_id_column = config.get("doc_id_column", "document_id")
        self.doc_content_column = config.get("doc_content_column", "contents")
        self.chunk_size = config.get("chunk_size", 0)
        self.text_lang = config.get("text_lang", "en")

        logger.debug(f"\n*** `config` to run:")
        for k,v in config.items():
            logger.debug(f"{k:20s}: {v}")

        # overwrite tokenizer:
        self.tokenizer = load_tokenizer(tokenizer_name=self.tokenizer)


    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        logger.debug(f"Transforming one table with {len(table)} rows using tokenizer {self.tokenizer}")

        # Tracking token count + document_id for non-empty row/doc:
        token_count = []
        processed_doc_ids = []

        # Track empty document_id of empty rows/docs:
        empty_doc_ids = []

        # #tokens per doc/row, eg: [[978, 1923, 313, 317], [317, 4294],...]
        doc_tokens = []

        for idx in range(table.num_rows):
            doc_id = table[self.doc_id_column][idx].as_py()
            doc_content = table[self.doc_content_column][idx].as_py()
            try:
                if self.chunk_size > 0 and len(doc_content) > self.chunk_size:
                    # tokenize document by chunks:
                    token_line = []
                    for chunk in split_text(doc_content,self.chunk_size, self.text_lang):
                        token_line.extend(self.tokenizer(chunk)["input_ids"])
                else:
                    token_line = self.tokenizer(doc_content)["input_ids"]
            except Exception as e:
                # skip failed row/doc, treat it as `empty` and move on:
                logger.warning(f"Failed in tokenizing `{doc_content}` due to:\n {e}")
                empty_doc_ids.append(doc_id)
                continue

            num_tokens = len(token_line)
            # skip empty document:
            if num_tokens == 0:
                empty_doc_ids.append(doc_id)
                continue
            else:
                doc_tokens.append(token_line)
                processed_doc_ids.append(doc_id)
                token_count.append(num_tokens)


        out_table = pa.table({"tokens": doc_tokens,
                              self.doc_id_column: processed_doc_ids,
                              "token_count": token_count})
        logger.debug(f"Done with the transformed table with {table.num_rows:,} rows")

        metadata = {"nfiles": 1,
                    "nrows": table.num_rows,
                    "ntokenizedrows": out_table.num_rows,
                    "nemptyrows": len(empty_doc_ids),
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
            "--tkn_tokenizer",
            type=str,
            default="bigcode/starcoder",
            help="Tokenizer used for tokenization. It also can be a path to a pre-trained tokenizer. By defaut, `bigcode/starcoder` from HuggingFace is used" ,
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

        parser.add_argument(
            "--tkn_text_lang",
            type=str,
            default="en",
            help="Specify language used in the text content for better text splitting if needed",
        )

        # This parameter may help to better tokenize very long doc/row (in chunks):
        parser.add_argument(
            "--tkn_chunk_size",
            type=int,
            default=0,
            help="Specify >0 value to tokenize each row/doc in chunks of characters (rounded in words)",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.tkn_tokenizer is None:
            raise RuntimeError(f"Parameter --tkn_tokenizer must be a valid tokenizer for tokenization, you specified {args.tkn_tokenizer}")

        if args.tkn_doc_id_column is None or args.tkn_doc_content_column is None:
            raise RuntimeError(f"Values for `--tkn_doc_id_column` and `--tkn_doc_content_column` must be provided")

        self.params["tokenizer"] = args.tkn_tokenizer
        self.params["doc_id_column"] = args.tkn_doc_id_column
        self.params["doc_content_column"] = args.tkn_doc_content_column
        self.params["text_lang"] = args.tkn_text_lang
        self.params["chunk_size"] = args.tkn_chunk_size

        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=TokenizationTransformConfiguration())
    logger.info("Launching Tokenization transform")
    launcher.launch()
