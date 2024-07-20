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

from enum import Enum
from typing import Any
from data_processing.utils import ParamsUtils, get_logger
import os
import json

default_configuration = f"{os.path.abspath(os.path.dirname(__file__))}/transform_configuration.json"
logger = get_logger(__name__)

def import_class(name):
    """
    Import class by name
    :param name: name
    :return:
    """
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def build_invoker_input(input_folder: str, output_folder: str, s3_config: dict[str, Any]) -> dict[str, Any]:
    """
    prepare location for data factory
    :param input_folder: input folder (local or S3)
    :param output_folder: output folder (local or S3)
    :param s3_config: S3 configuration - None local data
    :return: data access factory config
    """
    if s3_config is None:
        logger.info("Using local data")
        return {
            "data_local_config": ParamsUtils.convert_to_ast({
                "input_folder": input_folder,
                "output_folder": output_folder,
            })}
    logger.info("Using data from S3")
    return {
        "data_s3_conf": ParamsUtils.convert_to_ast({
            "input_folder": input_folder,
            "output_folder": output_folder,
        }),
        "data_s3_config": ParamsUtils.convert_to_ast(s3_config)
    }



# Supported runtimes
class TransformRuntime(Enum):
    """
    Supported runtimes and extensions
    """
    PYTHON = "python"
    RAY = "ray"
    SPARK = "spark"


class TransformsConfiguration:
    """
    Configurations for existing transforms
    """
    def __init__(self, configuration_file: str = default_configuration):
        """
        Initialization - loading transforms dictionary
        :param configuration_file: default is from the same dictionary
        """
        logger.info(f"loading from transforms configuration from {configuration_file}")
        with open(configuration_file) as f:
            self.transforms = json.load(f)


    def get_available_transforms(self) -> list[str]:
        """
        Get available transforms
        :return: a list of transform names
        """
        return list(self.transforms.keys())

    def get_configuration(self, transform: str, runtime: TransformRuntime = TransformRuntime.PYTHON)\
            -> tuple[str, str, list[str], str]:
        """
        Get configuration for a given transform/runtime
        :param transform: transform name
        :param runtime: runtime
        :return: a tuple containing transform subdirectory, library name, list of extra libraries
                 and configuration class name. Return None if transform/runtime does not exist
        """
        config = self.transforms.get(transform, None)
        if config is None:
            logger.warning(f"transform {transform} is not defined")
            return None, None, [], None
        match runtime:
            case TransformRuntime.PYTHON:
                # runtime Python
                if config[1] is None or config[7] is None:
                    logger.warning(f"transform {transform} for Python is not defined")
                    return None, None, [], None
                return config[0] + "python", config[1], config[2], config[7]
            case TransformRuntime.RAY:
                # runtime Ray
                if config[3] is None or config[8] is None:
                    logger.warning(f"transform {transform} for Ray is not defined")
                    return None, None, [], None
                return config[0] + "ray", config[3], config[4], config[8]
            case TransformRuntime.SPARK:
                # runtime Spark
                if config[5] is None or config[9] is None:
                    logger.warning(f"transform {transform} for Spark is not defined")
                    return None, None, [], None
                return config[0] + "spark", config[5], config[6], config[9]
            case _:
                logger.warning(f"undefined runtime {runtime.name}")
                return None, None, [], None
