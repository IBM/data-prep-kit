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

import pyarrow as pa


class AbstractTableTransform:
    """
    Converts input to output table
    Sub-classes must provide the transform() method to provide the conversion of one table to 0 or more new tables.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        self.config = config

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Converts input table into an output table.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :param table: input table
        :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
        propagated to metadata
        """
        raise NotImplemented()

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics. The majority of transformers
        should use default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
        propagated to metadata
        """
        return [], {}
