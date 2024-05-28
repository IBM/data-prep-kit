from typing import Any

from data_processing.transform.abstract_transform import DATA, AbstractTransform
from pyspark.sql import DataFrame


class AbstractSparkTransform(AbstractTransform[DataFrame]):  # todo: Remove double quotes
    def __init__(self, config: dict):
        self.config = config

    def transform(self, data: DataFrame) -> tuple[list[DataFrame], dict[str, Any]]:
        raise NotImplemented()
