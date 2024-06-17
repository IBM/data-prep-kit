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

import re
from typing import Any

from data_processing.utils import get_logger
from transformers import AutoTokenizer


logger = get_logger(__name__)


def split_text(text: str, chunk_size: int) -> str:
    """
    This function splits a given text into chunks and returns them one by one through yielding.
    It can be beneficial for tokenizing a very long text (comprising tens of thousands of words)
    where when some tokenizer may run sluggishly on the entire text.

    :param text: a very long document
    :param chunk_size: specified as the number of characters,
            although chunks are rounded by words, ensuring that
            the last word in a chunk remains intact and is not split into halves.
    :return: yielding a chunk of text each time.
    """

    # Additional languages without spaces among words can be added,
    # and each language may receive distinct treatment in word splitting.
    return _split_text_with_word_space(text, chunk_size)


def _split_text_with_word_space(text: str, chunk_size: int) -> str:
    """
    Split text into chunks for languages with spaces between words.
    For input/output, please refer to the split_text() function
    """
    index = 0
    while index < len(text):
        # last chunk:
        if len(text) - index < chunk_size:
            yield text[index:]
            break

        if text[index + chunk_size - 1] != " ":
            """
            if the last character of the chunk is not a space,
            search index of the last space in the chunk:
            """
            last_space_index = text.rfind(" ", index, index + chunk_size)

            if last_space_index != -1:  # s[last_space_index] = ' '
                # If found, return the chunk up to and include such space:
                yield text[index : last_space_index + 1]
                index = last_space_index + 1
            else:
                # If not, force cutting up to chunk_size:
                yield text[index : index + chunk_size]
                index += chunk_size
        else:
            yield text[index : index + chunk_size]
            index += chunk_size


def string_to_kwargs(string):
    """
    Convert a given string into kwargs dictionary

    :param string: a string of key_value parameters, for example:
         string = "cache_dir=/tmp/hf,use_auth_token=Your_HF_authentication_token"
    :return:
         kwargs_dict: a dictionary of key:value pairs
    """

    # Split string by commas to separate key-value pairs
    pairs = string.split(",")

    kwargs_dict = {}
    for pair in pairs:
        # Split by '=' to get key and value to be added to kwargs_dict:
        key, value = pair.split("=")
        kwargs_dict[key.strip()] = value.strip()

    return kwargs_dict


def load_tokenizer(tokenizer_name: str, tokenizer_args: dict) -> Any:
    """
     Load and return a tokenizer specified in `tokenizer_name`
    This function is designed to accommodate the loading of any tokenizer compatible with
    the Huggingface `AutoTokenizer` library, such as `bigcode/starcoder`, `Rocketknight1/falcon-rw-1b`, and others.

    Extending this function to support other customized tokenizers is straightforward.
    :param tokenizer_name: name of tokenizer. It can be one downloadable from HuggingFace or from
                          a locally specified folder.
    :param tokenizer_args: Arguments for creating tokenizer
    :return: a tokenizer
    """
    try:
        if tokenizer_args is not None:
            logger.debug(f"Load tokenizer `{tokenizer_name}` with arguments `{tokenizer_args}`...")
            kwargs = string_to_kwargs(tokenizer_args)
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_name, **kwargs)
        else:
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
    except Exception as e:
        logger.error(f"Failed to load tokenizer from `{tokenizer_name}` with  `HF AutoTokenizer`")
        raise RuntimeError(e)

    return tokenizer


def is_valid_argument_string(s: str) -> bool:
    """
    This is to initially check whether a given string contains valid key=value pairs
    for arguments of a tokenizer.
    :param s: string that will be checked whether being valid
    to be converted to key=value pairs. An example of a valid input string:
        s="cache_dir=/tmp/hf,use_auth_token=hf_huZIVsYjFNYBiZAjLapBngwSlgP"
    :return: boolean indicating whether s is valid string for arguments
    """

    pattern = r"^\s*\w+\s*=\s*\S+\s*$"

    # split s into pairs based on comma:
    pairs = s.split(",")

    # check valid of key=value pair:
    for pair in pairs:
        if not re.match(pattern, pair):
            return False
        # Additional check for consecutive equal signs:
        if "==" in pair:
            return False
    return True
