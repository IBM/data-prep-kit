import time

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.utils import get_logger
from html2parquet_transform import Html2ParquetTransformConfiguration


logger = get_logger(__name__)


class Html2ParquetPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for HTML2PARQUET as required by the PythonTransformLauncher.
    """
    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(transform_config=Html2ParquetTransformConfiguration())

if __name__ == "__main__":
    launcher = PythonTransformLauncher(Html2ParquetPythonTransformConfiguration())
    logger.info("Launching html2parquet transform")
    launcher.launch()