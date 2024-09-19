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

from typing import Any

from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics


class BaseTransformRuntime:
    """
    Base Transformer runtime used by processor to to create Transform specific environment
    Every Runtime defines specific implementation of this class
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TableTansformConfiguration.
        """
        self.params = params
