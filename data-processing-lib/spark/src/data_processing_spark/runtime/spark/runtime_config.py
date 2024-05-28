from data_processing.runtime import TransformRuntimeConfiguration
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform
from data_processing_spark.runtime.spark.spark_transform_config import (
    SparkTransformConfiguration,
)


class SparkTransformRuntimeConfiguration(TransformRuntimeConfiguration):
    def __init__(self, transform_config: SparkTransformConfiguration):
        self.transform_config = transform_config
