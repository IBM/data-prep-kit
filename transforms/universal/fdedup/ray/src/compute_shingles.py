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


def compute_shingles(text: str, word_shingle_size: int, delimiter: str = " ") -> list[str]:
    """
    Generate word shingles
    :param text: document
    :param delimiter: delimiter to split document
    :param word_shingle_size: size of shingle in words
    :return: list of shingles
    """
    separators = _find(text, delimiter)
    if len(separators) + 1 <= word_shingle_size:
        return [text]
    bounds = [-1] + separators + [len(text)]
    return [text[bounds[i] + 1 : bounds[i + word_shingle_size]] for i in range(0, len(bounds) - word_shingle_size)]
