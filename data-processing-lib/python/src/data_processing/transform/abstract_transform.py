# SPDX-License-Identifier: Apache-2.0
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

from abc import ABC, abstractmethod
import pyarrow as pa
from typing import Any

from data_processing.utils import TransformCategory, TransformConstants


class AbstractTransform(ABC):
    """
    Base class for all transform types
    """
    short_name: str = None
    category: TransformCategory = None

    config: dict[str, Any]  # type hint for clarity and IDE support

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This simply stores the given instance in this instance for later use.
        """
        if config is None:
            raise ValueError("config cannot be None")

        self.config = config.copy()  # optional: copy to avoid mutating external dict
        self.name = config.get(TransformConstants.NAME)
        self.id = config.get(TransformConstants.ID)
        self.job_id = config.get(TransformConstants.JOB_ID)
        self.job_run_id = config.get(TransformConstants.JOB_RUN_ID)
        self.context_id = config.get(TransformConstants.CONTEXT_ID, self.job_id)
        self.output_features_to_drop = config.get(TransformConstants.OUTPUT_FEATURES_TO_DROP, [])

    def validate(self, **kwargs) -> None:
        """
        Preform parameters validation.
        Subclasses should provide real implementation
        """
        pass

    @staticmethod
    def is_available() -> bool:
        """
        Disable the transform using this function.
        """
        return True

    def get_required_features(self):
        # The concrete subclasses will retrieve the required features.
        return []

    def get_metadata(self) -> dict:
        """
        Return the transform matadata
        """
        return {}

    def get_removed_and_added_columns(self) -> (set[str], list[pa.Field]):
        """
        Return the details of the removed and added columns after the transform.

        Returns:
            removed_columns (set[str]): Set of column names removed during the transform.
            added_columns (list[pa.Field]): List of column schemas added during the transform.
        """
        # TODO should we use NotImplementedError('subclasses must implement this method') instead
        return set(), []

    def get_metadata_fields_to_accumulate(self) -> list[str]:
        """
        Return the metadata field names that needs to be accumulated.
        Subclasses should provide real implementation

        Returns:
            list[str]: List of field to be accumulated.
        """
        # TODO should we use NotImplementedError('subclasses must implement this method') instead
        return []
