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
    Contain a number of functions to compute some statistics from documents
    based on Gopher paper (mostly in pages 39-41) https://arxiv.org/pdf/2112.11446.pdf
"""

# for particular Japanese language:
import re


hirakata_pat = re.compile(r"[ぁ-んァ-ン]")  # pattern for Japanese alphabets (Hiragana ぁ-ん and Katakana ァ-ン)
kuten_pat = re.compile(r"。")


def compute_word_statistics(text: str, symbols: list = ["#", "..."]) -> tuple[int, int, float]:
    """
    Given a text document:
        - Count the total number of words (should be between 50 and 100,000 words)
        - Compute mean of words' lengths (should be between 3 to 10 characters)
        - Count ratio of symbol-to-word ratio (should be <= 0.1) for either the hash symbol or the ellipsis
            Reference for symbols (emojis, etc.)
            https://textacy.readthedocs.io/en/0.11.0/api_reference/preprocessing.html

    """
    words = text.split()
    total_words = len(words)
    word_with_symbol_count = 0
    total_words_len = 0
    for word in words:
        total_words_len += len(word)
        word_with_symbol_count += 1 if any(symbol in word for symbol in symbols) else 0

    mean_word_len = total_words_len / total_words if total_words > 0 else 0
    symbol_to_word_ratio = word_with_symbol_count / total_words if total_words > 0 else 0

    return total_words, mean_word_len, symbol_to_word_ratio


def compute_bullet_point_ellipsis_alphabet_word_ratio(
    text: str, bullets: list = ["-", "*"]
) -> tuple[float, float, float]:
    """
    Given a text document:
        - Compute the ratio of lines starting with a bullet point (should be <=90%)
        - Compute the ratio of lines ending with an ellipsis (should be <=30%)
        - Compute the ratio of words having at least one alphabetic character (should be >80%)
    """

    lines = text.split("\n")
    total_lines = len(lines)

    # Count number of lines starting with a bullet point:
    count_bullet_lines = 0
    count_ellipsis_lines = 0
    for line in lines:
        count_bullet_lines += 1 if any(line.startswith(bullet) for bullet in bullets) else 0
        count_ellipsis_lines += 1 if line.endswith("...") else 0

    # Calculate the ratio of lines starting with a bullet point:
    bullet_point_ratio = count_bullet_lines / total_lines if total_lines > 0 else 0

    # Calculate the ratio of lines ending with the ellipsis:
    ellipsis_line_ratio = count_ellipsis_lines / total_lines if total_lines > 0 else 0

    words = text.split()

    # Count the number of words having at least one alphabetic character
    alphabet_count = sum(1 for word in words if any(c.isalpha() for c in word))

    alphabet_word_ratio = (alphabet_count / len(words)) if len(words) > 0 else 0

    return bullet_point_ratio, ellipsis_line_ratio, alphabet_word_ratio


def contains_common_English_words(text: str, ft_lang: str, min_count: int = 2) -> bool:
    """
    To return whether the given `text` contains at least `min_count` of the below common English words
    """
    if ft_lang != "en":
        return False

    common_English_words = ["the", "and", "to", "that", "of", "with", "be", "have"]
    count = 0
    for word in common_English_words:
        if text.count(word) > 0:
            count += 1
        if count == min_count:
            return True
    return False


"""
Extra heuristic rules for particular Japanese language
"""


def find_first_japanese_alphabet_position(text: str) -> int:
    """
    Return first position of occurrence of Japanese alphabets (i.e., Hiragana or Katakana)
    in an input `text`. (The returned value should be 0 <= pos <= 50)
    As Japanese alphabets are likely to occur in ordinary Japanese texts at some rates,
    this function checks and returns the first occurrence of Japanese alphabets.
    For example, text whose first occurrence of Japanese alphabets is in between 0 and 50
    can be considered as a normal Japanese text.
    This function is inspired by an OSS HojiChar (see the reference below).
    Example:
        find_first_japanese_alphabet_position('今日の天気は晴れ。')
        2
        find_first_japanese_alphabet_position('成田空港第1ターミナルに向かう。')
        6
        find_first_japanese_alphabet_position('本日晴天也。')
        -1
        Reference: https://github.com/HojiChar/HojiChar/blob/04a52de6d59e4d28dd98118d4db85970388fd1d6/hojichar/filters/document_filters.py#L504C1-L530C19
    """
    res = hirakata_pat.search(text)
    if res is None:
        return -1  # never appear
    else:
        return res.start()


def compute_average_japanese_sentence_length(text: str) -> int:
    """
    Compute average sentence length for an input `text`. (The returned value should be <= 250)
    A Japanese sentence is defined as a text between Japanese periods, called kutens, '。'
    in this function, and an average sentence length is defined as
    (the length of the input text) / (the number of Japanese periods).
    For example, text whose average sentence length is moderate, e.g., <= 250
    can be considered as a normal Japanese text.
    This function is inspired by an OSS HojiChar (see the reference below).
    Example:
        compute_average_japanese_sentence_length('今日の天気は晴れ。')
        9.0
        compute_average_japanese_sentence_length('今日の天気は晴れ。'*30)
        9.0
        compute_average_japanese_sentence_length('今日の天気は晴れ\n'*30)
        270
        Reference: https://github.com/HojiChar/HojiChar/blob/04a52de6d59e4d28dd98118d4db85970388fd1d6/hojichar/filters/document_filters.py#L533C1-L559C19
    """
    kutens = kuten_pat.findall(text)
    if len(kutens) == 0:
        return len(text)
    else:
        avg_sent_len = int(len(text) / len(kutens))
    return avg_sent_len
