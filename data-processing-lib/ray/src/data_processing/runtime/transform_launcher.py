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

import sys
from typing import Any

from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.runtime import TransformRuntimeConfiguration
from data_processing.utils import ParamsUtils, get_logger


logger = get_logger(__name__)


class AbstractTransformLauncher:
    def __init__(
        self,
        runtime_config: TransformRuntimeConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        self.runtime_config = runtime_config
        self.name = self.runtime_config.get_name()
        self.data_access_factory = data_access_factory

    def launch(self):
        raise ValueError("must be implemented by subclass")

    def get_transform_name(self) -> str:
        return self.name


def multi_launcher(params: dict[str, Any], launcher: AbstractTransformLauncher) -> int:
    """
    Multi launcher. A function orchestrating multiple launcher executions
    :param params: A set of parameters containing an array of configs (s3, local, etc)
    :param launcher: An actual launcher for a specific runtime
    :return: number of launches
    """
    # find config parameter
    config = ParamsUtils.get_config_parameter(params)
    if config is None:
        return 1
    # get and validate config value
    config_value = params[config]
    if type(config_value) is not list:
        logger.warning("config value is not a list")
        return 1
    # remove config key from the dictionary
    launch_params = dict(params)
    del launch_params[config]
    # Loop through all parameters
    n_launches = 0
    for conf in config_value:
        # populate individual config and launch
        launch_params[config] = conf
        sys.argv = ParamsUtils.dict_to_req(d=launch_params)
        res = launcher.launch()
        if res > 0:
            logger.warning(f"Launch with configuration {conf} failed")
        else:
            n_launches += 1
    return n_launches
