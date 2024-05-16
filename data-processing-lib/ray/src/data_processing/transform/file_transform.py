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

from typing import Any
from data_processing.transform import AbstractTransform


class AbstractFileTransform(AbstractTransform):
    """
    Converts input binary file to output file(s) (binary)
    Sub-classes must provide the transform() method to provide the conversion of one binary files to 0 or
    more new binary files.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        self.config = config

    def transform(self, file: bytes, ext: str) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :param file: input file
        :param ext: file extension
        :return: a tuple of a list of 0 or more converted file and a dictionary of statistics that will be
                 propagated to metadata
        """
        raise NotImplemented()

    def flush(self) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics. The majority of transformers
        should use default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :return: a tuple of a list of 0 or more converted file and a dictionary of statistics that will be
                 propagated to metadata
        """
        return [], {}
