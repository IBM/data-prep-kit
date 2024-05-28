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

from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.utils import get_logger


logger = get_logger(__name__)


class AbstractDataLauncher:
    def __init__(
        self,
        name: str,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        self.name = name
        self.data_access_factory = data_access_factory

    def launch(self):
        raise ValueError("must be implemented by subclass")

    def get_transform_name(self) -> str:
        return self.name


# def multi_launcher(params: dict[str, Any], launcher: AbstractDataLauncher) -> int:
#     .abstract_launcher.multi_launcher(params,launcher)
