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

from data_processing.transform import TransformConfiguration
from data_processing.transform.abstract_transform import DATA, AbstractTransform
from data_processing.utils import CLIArgumentProvider
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform


class SparkTransformConfiguration(TransformConfiguration):
    def __init__(self, name: str, transform_class: type[AbstractSparkTransform], remove_from_metadata: list[str] = []):
        super().__init__(name, transform_class, remove_from_metadata)
