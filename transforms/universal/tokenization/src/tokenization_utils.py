from data_processing.utils import get_logger
import os, sys

import re

logger = get_logger(__name__)

# local_tokenizer = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tokenizers", "gpt2_based"))

from transformers import AutoTokenizer


def split_text(text: str, chunk_size: int, text_lang: str, reserve_consecutive_linebreaks: bool = True) -> str:
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
    if text_lang in ['ja', 'zh']:
        return _split_text_wout_word_space(text, chunk_size, reserve_consecutive_linebreaks)
    else:
        return _split_text_with_word_space(text, chunk_size, reserve_consecutive_linebreaks)


def _split_text_with_word_space(text: str, chunk_size: int, reserve_consecutive_linebreaks: bool = True) -> str:
    '''
    Split text into multiple chunks of characters, rounded by words, for languages with spaces between words.
    For input/output, please refer to the split_text() function
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


def _split_text_wout_word_space(text: str, chunk_size: int, reserve_consecutive_linebreaks: bool = True) -> str:
    '''
    Split the text into multiple chunks for some specific languages without spaces between words.
    This version is preliminary and necessitates further development for each respective language.
    For input/output, please refer to the split_text() function
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


def load_tokenizer(tokenizer_name: str):
    """
    Load and return a tokenizer specified in `tokenizer_name`
    This function is designed to accommodate the loading of any tokenizer compatible with
    the Huggingface `AutoTokenizer` library, such as `bigcode/starcoder`, `Rocketknight1/falcon-rw-1b`, and others.

    Extending this function to support other customized tokenizers is straightforward.
    :param tokenizer_name: name of tokenizer. It can be one downloadable from HuggingFace or from a locally specified folder.
    :return: a tokenizer
    """
    try:
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
    except Exception as e:
        raise RuntimeError(f"Failed to load tokenizer from `{tokenizer_name}` with  `HF AutoTokenizer` due to\n: {e}")

    return tokenizer
