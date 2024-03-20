"""
Libraries need to be added to venv:
    transformers==4.35.0
"""

import time
from argparse import ArgumentParser, Namespace
from typing import Any
import re

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

# local_tokenizer = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tokenizers", "gpt2_based"))

from transformers import AutoTokenizer

def split_text(text:str,chunk_size:int,text_lang:str,reserve_consecutive_linebreaks:bool=True) -> str:
    """
    This function splits the given (particularly lengthy) text into chunks and returns them one by one through yielding.
    It can be beneficial for processing very long texts (comprising tens of thousands of words)
    where tokenization by a tokenizer may run sluggishly.

    :param text: a long document
    :param chunk_size: specified as the number of characters,
            although chunks are rounded by words, ensuring that
            the last word in a chunk remains intact and is not split into halves.
    :param text_lang: a standard acronym for each language, eg, `en`, `vi`, `ja`, etc.
    :param reserve_consecutive_linebreaks:
        Set to true to preserve multiple consecutive line breaks in the given text.
        Set to false to preserve only one line break for multiple consecutive line breaks.
    :return: yielding a chunk of text each time.
    Example:
        text = "This is the first line.\n\n This is the 2nd line after 02 line breaks."
        for chunk in split_text(text=text,chunk_size=25,text_lang='en'):
            print(f"{len(chunk):3,}: {chunk}")
        return:
         23: This is the first line.
          1:

          1:

         20: This is the 2nd line
         21: after 02 line breaks.
    """


    # Additional languages without spaces among words can be added, and each language may receive distinct treatment in word splitting.
    if text_lang in ['ja','zh']:
        return split_text_wout_word_space(text,chunk_size,reserve_consecutive_linebreaks)
    else:
        return split_text_with_word_space(text,chunk_size,reserve_consecutive_linebreaks)

def split_text_with_word_space(text:str,chunk_size:int,reserve_consecutive_linebreaks:bool=True) -> str:
    '''
    Split text into multiple chunks of characters, rounded by words, for languages with spaces between words.
    '''
    lines = text.split('\n')
    for i, line in enumerate(lines):
        current_chunk = ''
        words = line.split()
        for j, word in enumerate(words):
            word += ' '
            if len(current_chunk) + len(word) <= chunk_size:
                current_chunk += word
            else:
                # current `word` is not the last one in `words`:
                yield current_chunk.strip()
                current_chunk = word
        if current_chunk:
            yield current_chunk.strip()

        if reserve_consecutive_linebreaks:
            # reserve multiple consecutive line breaks in the original text:
            if i < len(lines) - 1:
                yield '\n'

def split_text_wout_word_space(text:str,chunk_size:int, reserve_consecutive_linebreaks:bool=True) -> str:
    '''
    Split the text into multiple chunks for some specific languages without spaces between words.
    This version is preliminary and necessitates further development for each respective language.
    '''
    lines = text.split('\n')
    for i, line in enumerate(lines):
        current_chunk = ''
        # capture words, spaces and line breaks through reg.pattern
        words = re.findall(r'\w+|[^\w\s]+|\n+', line, re.UNICODE)
        for j, word in enumerate(words):
            if len(current_chunk) + len(word) <= chunk_size:
                current_chunk += word
            else:
                yield current_chunk
                current_chunk = word
        if current_chunk:
            yield current_chunk

        if reserve_consecutive_linebreaks:
            if i < len(lines) - 1:
                yield '\n'

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
        self.tokenizer_path = config.get("tokenizer_path", "bigcode/starcoder")
        self.doc_id_column = config.get("doc_id_column", "document_id")
        self.doc_content_column = config.get("doc_content_column", "contents")
        self.chunk_size = config.get("chunk_size", 0)
        self.text_lang = config.get("text_lang", "en")

        logger.info(f"\n*** `config` to run:")
        for k,v in config.items():
            logger.info(f"{k:20s}: {v}")

        self.tokenizer = self._load_tokenizer(do_testing=False)


    def _load_tokenizer(self,do_testing:bool=False):
        """
        Load and return a tokenizer.
        This function is designed to accommodate the loading of any tokenizer compatible with
        the Huggingface `AutoTokenizer` library, such as `bigcode/starcoder`, `Rocketknight1/falcon-rw-1b`, and others.
        The tokenizer can be obtained either by direct download from HuggingFace or from a locally specified folder.
        Extending this function to support other customized tokenizers is straightforward.
        """
        try:
            tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_path)
        except Exception as e:
            sys.exit(f"Failed to load tokenizer from `{self.tokenizer_path}` with  `HF AutoTokenizer` due to\n: {e}")

        # quick test tokenizer:
        if do_testing:
            txt = "This text is for testing purpose!"
            token_line = tokenizer(txt)["input_ids"]
            print(f"== {self.tokenizer_path} has tokenized `{txt}` to: {token_line}")

        return tokenizer

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        logger.debug(f"Transforming one table with {len(table)} rows using tokenizer {self.tokenizer_path}")

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
                        print(f"== {chunk}")
                        token_line.extend(self.tokenizer(chunk)["input_ids"])
                else:
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
                processed_doc_ids.append(doc_id)
                token_count.append(num_tokens)


        out_table = pa.table({"tokens": doc_tokens,
                              self.doc_id_column: processed_doc_ids,
                              "token_count": token_count})
        logger.debug(f"Done with the transformed table with {table.num_rows:,} rows")

        metadata = {"num_files": 1,
                    "num_rows": table.num_rows,
                    "num_tokenized_rows": out_table.num_rows,
                    "num_empty/failed_rows": len(empty_doc_ids),
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
            default="bigcode/starcoder",
            help="Tokenizer used for tokenization. It also can be a path to a pre-trained tokenizer. By defaut, it is the `bigcode/starcoder` from HuggingFace",
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
            help="Specify >0 value to tokenize each row/doc in chunks of characters (round in words)",
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
        self.params["text_lang"] = args.tkn_text_lang
        self.params["chunk_size"] = args.tkn_chunk_size

        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=TokenizationTransformConfiguration())
    logger.info("Launching Tokenization transform")
    launcher.launch()


