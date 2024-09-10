# (C) Copyright IBM Corp. 2024.
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
Libraries need to be added to venv:
    transformers==4.35.0
"""

import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from tokenization_utils import is_valid_argument_string, load_tokenizer, split_text


CHUNK_CHECKPOINT_INTERVAL = 100


class TokenizationTransform(AbstractTableTransform):
    """ """

    def __init__(self, config: dict[str, Any]):
        """
        This class is used to transform an input table to an output table utilizing a tokenizer.
        The input table must contain at least two columns, with default names set as `document_id` and `contents`.
        The tokenizer will tokenize each row in `contents` into a sequence of token_ids and write it to `tokens` column
        in the output table, along with the document id and token count stored respectively in `document_id`
        and `token_count` column.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of TokenizationTransformConfiguration class

        super().__init__(config)
        self.tokenizer = config.get("tokenizer", "hf-internal-testing/llama-tokenizer")
        self.tokenizer_args = config.get("tokenizer_args", None)
        self.doc_id_column = config.get("doc_id_column", "document_id")
        self.doc_content_column = config.get("doc_content_column", "contents")
        self.chunk_size = config.get("chunk_size", 0)
        self.text_lang = config.get("text_lang", "en")

        self.logger.debug(f"\n*** `config` to run:")
        for k, v in config.items():
            self.logger.debug(f"{k:20s}: {v}")

        # overwrite tokenizer:
        self.tokenizer = load_tokenizer(tokenizer_name=self.tokenizer, tokenizer_args=self.tokenizer_args)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows using tokenizer {self.tokenizer}")

        # Tracking token count + document_id for non-empty row/doc:
        token_count = []
        processed_doc_ids = []

        # Track empty document_id of empty rows/docs:
        empty_doc_ids = []

        # num. of tokens per doc/row, eg: [[978, 1923, 313, 317], [317, 4294],...]
        doc_tokens = []

        # document length in #characters:
        doc_lengths = []

        for idx in range(table.num_rows):
            doc_id = table[self.doc_id_column][idx].as_py()
            doc_content = table[self.doc_content_column][idx].as_py()
            doc_length = len(doc_content)

            # skip empty document/row:
            if doc_length == 0:
                empty_doc_ids.append(doc_id)
                continue

            try:
                if self.chunk_size > 0 and doc_length > self.chunk_size:
                    # tokenize document by chunks:
                    start_time = time.time()
                    token_line = []
                    doc_len_so_far = 0
                    for chunk_idx, chunk in enumerate(split_text(doc_content, self.chunk_size)):
                        token_line.extend(self.tokenizer(chunk)["input_ids"])
                        doc_len_so_far += len(chunk)

                        if (chunk_idx + 1) % CHUNK_CHECKPOINT_INTERVAL == 0 or (doc_len_so_far == doc_length):
                            elapse_time = int(time.time() - start_time)
                            self.logger.debug(
                                f"row_idx: {idx:5,} "
                                f"(doc_id: {doc_id}) "
                                f"chunk_idx: {chunk_idx:6,} ({doc_len_so_far:11,}/{doc_length:11,} "
                                f"{100*doc_len_so_far/doc_length:5.1f}%) #tokens: {len(token_line):9,} "
                                f"elapse_time:{elapse_time: .1f}(s)"
                            )
                else:
                    token_line = self.tokenizer(doc_content)["input_ids"]
            except Exception as e:
                # skip failed row/doc, treat it as `empty` and move on:
                self.logger.warning(f"Failed in tokenizing `{doc_content}` due to:\n {e}")
                empty_doc_ids.append(doc_id)
                continue

            num_tokens = len(token_line)
            # skip document with empty returned tokens:
            if num_tokens == 0:
                empty_doc_ids.append(doc_id)
                continue
            else:
                doc_lengths.append(doc_length)
                doc_tokens.append(token_line)
                processed_doc_ids.append(doc_id)
                token_count.append(num_tokens)

        out_table = pa.table(
            {
                "tokens": doc_tokens,
                self.doc_id_column: processed_doc_ids,
                "document_length": doc_lengths,
                "token_count": token_count,
            }
        )
        self.logger.debug(f"Done with the transformed table with {table.num_rows:,} rows")

        metadata = {
            "num_files": 1,
            "num_rows": table.num_rows,
            "num_tokenized_rows": out_table.num_rows,
            "num_empty_rows": len(empty_doc_ids),
            "num_tokens": sum(token_count),
            "num_chars": sum(doc_lengths),
        }

        return [out_table], metadata


class TokenizationTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name="Tokenization",
            transform_class=TokenizationTransform,
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

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
            default="hf-internal-testing/llama-tokenizer",
            help="Tokenizer used for tokenization. It also can be a path to a pre-trained tokenizer. By default, `hf-internal-testing/llama-tokenizer` from HuggingFace is used",
        )

        parser.add_argument(
            "--tkn_tokenizer_args",
            type=str,
            default=None,
            help="Arguments for tokenizer. For example, `cache_dir=/tmp/hf,use_auth_token=Your_HF_authentication_token` could be arguments for `bigcode/starcoder`",
        )

        parser.add_argument(
            "--tkn_doc_id_column",
            type=str,
            default="document_id",
            help="Column contains document id which values should be unique across dataset",
        )

        parser.add_argument(
            "--tkn_doc_content_column",
            type=str,
            default="contents",
            help="Column contains document content",
        )

        parser.add_argument(
            "--tkn_text_lang",
            type=str,
            default="en",
            help="Specify language used in text content for better text splitting if needed",
        )

        # This parameter may help to better tokenize very long doc/row (in chunks):
        parser.add_argument(
            "--tkn_chunk_size",
            type=int,
            default=0,
            help="Specify >0 value to tokenize each row/text in chunks of characters (rounded in words)",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.tkn_tokenizer is None:
            self.logger.error(
                f"Parameter --tkn_tokenizer must be a valid tokenizer for "
                f"tokenization, you specified {args.tkn_tokenizer}"
            )
            return False

        # set arguments for tokenizer to None if it is an empty string (supporting KFP):
        if args.tkn_tokenizer_args == "":
            args.tkn_tokenizer_args = None

        if args.tkn_tokenizer_args is not None:
            if not is_valid_argument_string(args.tkn_tokenizer_args):
                self.logger.error(
                    f"Parameter --tkn_tokenizer_args must be a valid argument string of `key1=value1,key2=value2`, you specified {args.tkn_tokenizer_args}"
                )
                return False

        if args.tkn_doc_id_column is None or args.tkn_doc_content_column is None:
            self.logger.error(f"Values for `--tkn_doc_id_column` and `--tkn_doc_content_column` must be provided")
            return False

        # For MVP1: only support english text:
        if args.tkn_text_lang != "en":
            self.logger.error(
                f"This version has not supported languages other than `en` yet, you specified {args.tkn_text_lang}"
            )
            return False

        self.params["tokenizer"] = args.tkn_tokenizer
        self.params["tokenizer_args"] = args.tkn_tokenizer_args
        self.params["doc_id_column"] = args.tkn_doc_id_column
        self.params["doc_content_column"] = args.tkn_doc_content_column
        self.params["text_lang"] = args.tkn_text_lang
        self.params["chunk_size"] = args.tkn_chunk_size

        return True
