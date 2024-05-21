from typing import Any

from data_processing.transform import TransformConfiguration
from data_processing.transform.abstract_transform import DATA, AbstractTransform
from data_processing.utils import CLIArgumentProvider
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform


class SparkTransformConfiguration(TransformConfiguration):
    def __init__(self, name: str, transform_class: type[AbstractSparkTransform], remove_from_metadata: list[str] = []):
        super().__init__(name, transform_class, remove_from_metadata)
