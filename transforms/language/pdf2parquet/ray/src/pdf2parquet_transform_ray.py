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

import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from pdf2parquet_transform import (
    Pdf2ParquetTransform,
    Pdf2ParquetTransformConfiguration,
)
from ray.util.metrics import Counter, Gauge


logger = get_logger(__name__)


class Pdf2ParquetRayTransform(Pdf2ParquetTransform):
    def __init__(self, config: dict):
        """ """
        super().__init__(config)

        self.doc_counter = Counter("worker_pdf_doc_count", "Number of PDF documents converted by the worker")
        self.page_counter = Counter("worker_pdf_pages_count", "Number of PDF pages converted by the worker")
        self.page_convert_gauge = Gauge(
            "worker_pdf_page_avg_convert_time", "Average time for converting a single PDF page on each worker"
        )
        self.doc_convert_gauge = Gauge("worker_pdf_convert_time", "Time spent converting a single document")

    def _update_metrics(self, num_pages: int, elapse_time: float):
        self.page_convert_gauge.set(elapse_time / num_pages)
        self.doc_convert_gauge.set(elapse_time)
        self.doc_counter.inc(1)
        self.page_counter.inc(num_pages)


class Pdf2ParquetRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for PDF2PARQUET as required by the RayTransformLauncher.
    """

    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(transform_config=Pdf2ParquetTransformConfiguration(transform_class=Pdf2ParquetRayTransform))


if __name__ == "__main__":
    launcher = RayTransformLauncher(Pdf2ParquetRayTransformConfiguration())
    logger.info("Launching pdf2parquet transform")
    launcher.launch()
