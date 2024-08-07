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
@ Description:
    Implement C4 heuristic rules applied to documents as presented in:
        + "Exploring the Limits of Transfer Learning with a Unified Text-to-Text Transformer" paper updated on 2023Sept19
            (C4 paper, page 6): https://arxiv.org/pdf/1910.10683.pdf
        + RedPajamaV2 https://together.ai/blog/redpajama-data-v2
"""


import re
import string

from cc_net_prepro import unicode_normalization
from doc_quality_utils import load_bad_words


TRANSLATION_TABLE_PUNCTUATION = str.maketrans("", "", string.punctuation)


def c4_text_normalization(
    text: str,
    ft_lang: str,
    remove_punct: bool = True,
    lowercase: bool = True,
    unicode_normalized: bool = True,
    white_space: bool = True,
) -> str:
    if remove_punct:
        # adopt RedPajamaV2 (instead of cc-net) for punctuation removal:
        text = text.translate(TRANSLATION_TABLE_PUNCTUATION)
        # text = _remove_unicode_punct(text)

    if lowercase:
        text = text.lower()

    if white_space:
        text = text.strip()
        text = re.sub(r"\s+", " ", text)

    if unicode_normalized:
        text = unicode_normalization(text, ft_lang)

    return text


def c4_sentence_count(text: str, ft_lang: str) -> int:
    """
    Return counting number of sentences from given `text`.
    In C4 paper (page6), it's recommended to remove any webpage with fewer than 3 sentences
    Currently support for ja and 5 european languages: de, en, es, fr, pt.
    """

    if ft_lang == "ja":
        kuten_pat = re.compile(r"。")
        kutens = kuten_pat.findall(text)
        return len(kutens)

    SENTENCE_PATTERN = re.compile(r"\b[^.!?]+[.!?]*", flags=re.UNICODE)
    if len(text) == 0:
        return 0
    else:
        return len(SENTENCE_PATTERN.findall(text))


def c4_contain_pattern_ratio(text: str, pattern: str, ft_lang: str, normalize_text: bool = True) -> float:
    """
    Return ratio between the number of occurrences of `pattern` over the text length
    It's recommended to remove any webpage having `pattern` like (per C4 paper, page 6):
        + "lorem ipsum".
        + curly bracket “{”, "}"
    """

    if normalize_text:
        text = c4_text_normalization(text, ft_lang)

    if len(text) == 0:
        return 0.0

    # search for curly bracket with re:
    if pattern == "{" or pattern == "}":
        pattern = "\\" + pattern

    SEARCH_REGEX = re.compile(pattern, re.IGNORECASE)

    pattern_count = len(SEARCH_REGEX.findall(text))
    if pattern_count == 0:
        return 0.0

    pattern_count_ratio = float(pattern_count) / len(text)
    return pattern_count_ratio


def c4_load_ldnoobw_words(ft_lang: str, file_path: str):
    """
    Compile and return search pattern once to use everywhere.
    """
    set_of_bad_words = load_bad_words(ft_lang, file_path=file_path)
    # print(f"== {file_path} has {len(set_of_bad_words)} words")
    # Create a re pattern to match any bad word:
    re_pattern = r"\b(?:" + "|".join(map(re.escape, set_of_bad_words)) + r")\b"
    return re_pattern


def c4_contains_ldnoobw_words(text: str, pattern) -> bool:
    """
    It's recommended to remove any webpage containing any word on the
    “List of Dirty, Naughty, Obscene or Otherwise Bad Words are obtained from:
    https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words
    TODO check licensing: Creative Commons Attribution 4.0 license
    """
    # Use re.search to find any bad word in the text:
    match = re.search(pattern, text, re.IGNORECASE)

    return match is not None


if __name__ == "__main__":

    text_en = "This javascript has Lorem ipsum. It also has a {test} string with {brackets}."
    print(f"== sentence count for en: {c4_sentence_count(text_en, ft_lang='en')}")

    curly_bracket_ratio = 0.0
    for sign in ["{", "}"]:
        curly_bracket_ratio += c4_contain_pattern_ratio(text_en, pattern=sign, ft_lang="en", normalize_text=False)

    print(f"== 2 curly bracket: {curly_bracket_ratio:.3f}")

    pattern = "}"
    print(f"== curly bracket: {c4_contain_pattern_ratio(text_en, pattern, ft_lang='en', normalize_text=False):.3f}")

    pattern = "Javascript"
    print(f"== javascript: {c4_contain_pattern_ratio(text_en, pattern, ft_lang='en', normalize_text=True):.3f}")

    pattern = "lorem ipsum"
    print(f"== lorem ipsum: {c4_contain_pattern_ratio(text_en, pattern, ft_lang='en', normalize_text=True):.3f}")

    text_ja = "成田空港第1ターミナルに向かう。Lorem ipsum lorem ipsum 。{成田空港第1タ}ーミナルに向かう。"
    pattern = "Lorem ipsum"
    print(f"== lorem ipsum: {c4_contain_pattern_ratio(text_ja, pattern, ft_lang='ja', normalize_text=True):.3f}")

    pattern = "}"
    print(f"== curly bracket ja: {c4_contain_pattern_ratio(text_ja, pattern, ft_lang='ja', normalize_text=False):.3f}")

    print(f"== sentence count for ja: {c4_sentence_count(text_ja,ft_lang='ja')}")

#    ft_lang = "en"
#    ldnoobw_filepath = "/tmp/ldnoobw/en"

#    offensive_text = "This is a sample text containing alabama hot pocket words."
#    print(
#        f"The text has bad word? {c4_contains_ldnoobw_words_ver01(offensive_text, ft_lang=ft_lang, file_path=ldnoobw_filepath)}"
#    )

#    pttn = c4_load_ldnoobw_words(ft_lang=ft_lang, file_path=ldnoobw_filepath)
#    print(f"The text has bad word? {c4_contains_ldnoobw_words(offensive_text, pttn)}")
