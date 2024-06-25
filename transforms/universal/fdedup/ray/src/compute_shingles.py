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

import string


"""
This implements the most simplistic splitting of document based on the white spaces
that can be overwritten by a different document splitter (tokenizer). This method is
build in the library and can be overwritten using approach described at 
https://stackoverflow.com/questions/37553545/how-do-i-override-a-function-of-a-python-library

import compute_shingles
compute_shingles.compute_shingles = my_local_compute_shingles
"""


def _find(s: str, ch: str) -> list[int]:
    """
    Get indexes of all locations of character in string
    :param s: string
    :param ch: character
    :return: list of locations
    """
    return [i for i, ltr in enumerate(s) if ltr == ch]


def compute_shingles(txt: str, word_shingle_size: int, delimiter: str = " ") -> list[str]:
    """
    Generate word shingles
    :param txt: document
    :param delimiter: delimiter to split document
    :param word_shingle_size: size of shingle in words
    :return: list of shingles
    """
    text = txt.replace("\n", "").lower().translate(str.maketrans("", "", string.punctuation))
    separators = _find(text, delimiter)
    if len(separators) + 1 <= word_shingle_size:
        return [text]
    bounds = [-1] + separators + [len(text)]
    return [text[bounds[i] + 1 : bounds[i + word_shingle_size]] for i in range(0, len(bounds) - word_shingle_size)]
