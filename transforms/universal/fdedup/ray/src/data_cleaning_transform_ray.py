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

import ray
from data_cleaning_transform import (
    DataCleaningTransform,
    DataCleaningTransformConfiguration,
    docs2remove_list,
    docs2remove_list_key,
    get_docs_to_remove,
)
from data_processing.data_access import DataAccessFactoryBase
from data_processing.utils import CLIArgumentProvider, get_logger
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle


logger = get_logger(__name__)


class DataCleaningRayTransform(DataCleaningTransform):
    """ """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, LangSelectorTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        docs2remove = config.get(docs2remove_list_key, None)
        if docs2remove is not None:
            # This is recommended for production approach. In this case domain list is build by the
            # runtime once, loaded to the object store and can be accessed by actors without additional reads
            try:

                config[docs2remove_list_key] = ray.get(config.get(docs2remove_list_key))
            except Exception as e:
                self.logger.warning(f"Exception loading languages list from ray object storage {e}")
                raise RuntimeError(f"exception loading from object storage for key {docs2remove}")
        super().__init__(config)


class DataCleaningRuntime(DefaultRayTransformRuntime):
    """
    Ingest Data cleaning runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            ingest_supported_langs_file_key: supported languages file
            ingest_detect_programming_lang_key: whether to detect programming language
            ingest_domain_key: domain
            ingest_snapshot_key: snapshot
        """
        super().__init__(params)
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def get_transform_config(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: ActorHandle,
        files: list[str],
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to remove
        :return: dictionary of filter init params
        """
        docs_to_remove = get_docs_to_remove(self.params)
        docs_to_remove_list = ray.put(docs_to_remove)
        return {docs2remove_list_key: docs_to_remove_list} | self.params


class DataCleaningRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for NOOP as required by the RayTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(
            transform_config=DataCleaningTransformConfiguration(transform_class=DataCleaningRayTransform),
            runtime_class=DataCleaningRuntime,
        )


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = RayTransformLauncher(DataCleaningRayTransformConfiguration())
    logger.info("Launching  transform")
    launcher.launch()
