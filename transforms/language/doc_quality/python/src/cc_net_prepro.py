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
@ Create on: 2023/04/25
@ Description:
    To incorporate preprocessing steps from cc_net/

@ Reference:
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py
    https://github.com/bigscience-workshop/data_tooling/blob/master/kenlm_training/cc_net/text_normalizer.py
"""

import re
import unicodedata
from typing import Dict


unicode_punct_dict: Dict[str, str] = {
    "，": ",",
    "。": ".",
    "、": ",",
    "„": '"',
    "”": '"',
    "“": '"',
    "«": '"',
    "»": '"',
    "１": '"',
    "」": '"',
    "「": '"',
    "《": '"',
    "》": '"',
    "´": "'",
    "∶": ":",
    "：": ":",
    "？": "?",
    "！": "!",
    "（": "(",
    "）": ")",
    "；": ";",
    "–": "-",
    "—": " - ",
    "．": ". ",
    "～": "~",
    "’": "'",
    "…": "...",
    "━": "-",
    "〈": "<",
    "〉": ">",
    "【": "[",
    "】": "]",
    "％": "%",
    "►": "-",
}

_unicode_punct_re = re.compile(f"[{''.join(unicode_punct_dict.keys())}]")

"""
Generate regex pattern obj for later searching using re.search() or re.match()
(r'[\x00\x01...\x9e\x9f]',re.UNICODE)
"""
_non_printing_chars_re = re.compile(f"[{''.join(map(chr, list(range(0, 32)) + list(range(127, 160))))}]")


def unicode_normalization(line: str, language="en") -> str:
    """
    For some languages like 'ja' or 'en', its text should be normalized with the right unicode format
    prior to calling tokenization by sentence piece tokenizer
    """
    if language == "ja":
        line = unicodedata.normalize("NFKC", line)
    elif language == "en":
        line = unicodedata.normalize("NFD", line)  # normalize line using Unicode Normalization Form D
    else:
        """
        TODO: add relevant unicodedata normalization here for other languages as needed
        """
        return line

    return line


def _strip_accents(line) -> str:
    """
    This currently is applied for `en` and `ja` language to strip out accents from text.
    The given text 'line' should be normalized with the right unicode format prior to calling this method.
    For example:
        line = "Café élevàtor ôperàtor naïve Noël façade don't"
            -> "Cafe elevator operator naive Noel facade don't"  if line was normalized with unicode format NFD
            -> "Cafe elevator operator naïve Noël façade don't"  if line was NOT normalized with any unicode format
    """

    """Keep char whose category is NOT "Mn", i.e, 
    "Mn" is category for Mark or Non-Spacing characters like diacritic/accent or non-spacing marks
    Example of some diacritic/non-spacing marks: ^,´,` as they don't occupy space
    """
    output = [c for c in line if unicodedata.category(c) != "Mn"]  # decompose line into chars and diacritical marks
    if len(output) == line:
        return line
    return "".join(output)


def _replace_unicode_punct(line: str) -> str:
    """
    Replace unicode punctuations defined in `unicode_punct_dict'
    """
    return "".join(unicode_punct_dict.get(c, c) for c in line)


def _remove_unicode_punct(line: str) -> str:
    """
    More aggressive _replace_unicode_punct
    """
    return _unicode_punct_re.sub("", line)


def _remove_non_printing_char(line: str) -> str:
    return _non_printing_chars_re.sub("", line)


def cc_net_normalize(
    line: str,
    strip_accent: bool = True,
    lower_case: bool = True,
    digit_2_zero: bool = True,
    punct_level: int = 1,
    language: str = "en",
) -> str:
    line = line.strip()

    if not line:
        return line

    line = unicode_normalization(line=line, language=language)

    if lower_case:
        line = line.lower()

    if strip_accent:
        line = _strip_accents(line)

    if digit_2_zero:
        # eg, "int 10 float 2.01 scientific 1.2e10" -> "int 00 float 0.00 scientific 0.0e00"
        line = re.compile(r"\d").sub("0", line)

    if punct_level == 1:
        line = _replace_unicode_punct(line)

    elif punct_level == 2:
        line = _remove_unicode_punct(line)

    line = _remove_non_printing_char(line)
    return line


if __name__ == "__main__":
    line = "Int 10 float 2.01 scientific 1.2e10 Café ôperàtor"
    new_line = cc_net_normalize(line)
    print(f"== {line} -> {new_line}")
