#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.utils import get_logger
from hap_transform import HAPTransformConfiguration
logger = get_logger(__name__)


class HAPPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for HAP as required by the PythonTransformLauncher.
    """
    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(transform_config=HAPTransformConfiguration())
        
if __name__ == "__main__":
    launcher = PythonTransformLauncher(HAPPythonTransformConfiguration())
    logger.info("Launching HAP transform")
    launcher.launch()    