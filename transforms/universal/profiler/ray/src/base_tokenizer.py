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
This implements the most simplistic tokenizer based on the white spaces
that can be overwritten by a different a different one. This method is
build in the library and can be overwritten using approach described at 
https://stackoverflow.com/questions/37553545/how-do-i-override-a-function-of-a-python-library

import base_tokenizer
base_tokenizer.tokenize = my_local_tokenize

"""


def tokenize(text: str) -> list[str]:
    """
    Tokenize string
    :param text: source text
    :return: list of tokens (words)
    """
    # start from normalizing string
    normal = text.strip().lower().translate(str.maketrans("", "", string.punctuation))
    return normal.split()
