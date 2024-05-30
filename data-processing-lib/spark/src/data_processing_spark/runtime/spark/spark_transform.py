# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import Any

from data_processing.transform.abstract_transform import DATA, AbstractTransform
from pyspark.sql import DataFrame


class AbstractSparkTransform(AbstractTransform[DataFrame]):  # todo: Remove double quotes
    def __init__(self, config: dict):
        self.config = config

    def transform(self, data: DataFrame) -> tuple[list[DataFrame], dict[str, Any]]:
        raise NotImplemented()
