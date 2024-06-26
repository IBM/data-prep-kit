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
from code2parquet_transform import (
    CodeToParquetTransform,
    CodeToParquetTransformConfiguration,
    data_factory_key,
    get_supported_languages,
    supported_langs_file_key,
    supported_languages_key,
)
from data_processing.data_access import DataAccessFactoryBase
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle


supported_languages_ref_key = "supported_languages_ref"


class CodeToParquetRayTransform(CodeToParquetTransform):
    """ """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, LangSelectorTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """

        supported_languages_ref = config.get(supported_languages_key, None)
        if supported_languages_ref is not None:
            # This is recommended for production approach. In this case domain list is build by the
            # runtime once, loaded to the object store and can be accessed by actors without additional reads
            try:
                self.logger.info(
                    f"Loading languages to include from Ray storage under reference {supported_languages_ref}"
                )
                languages_supported = ray.get(supported_languages_ref_key)
                config[supported_languages_key] = languages_supported
            except Exception as e:
                self.logger.warning(f"Exception loading languages list from ray object storage {e}")
                raise RuntimeError(f"exception loading from object storage for key {supported_languages_ref}")
        super().__init__(config)


class CodeToParquetRuntime(DefaultRayTransformRuntime):
    """
    Ingest to Parquet runtime support
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
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        lang_file = self.params.get(supported_langs_file_key, None)
        if lang_file is None:
            raise RuntimeError(f"Missing configuration key {supported_langs_file_key}")
        lang_data_access_factory = self.params.get(data_factory_key, None)
        if lang_data_access_factory is None:
            raise RuntimeError(f"Missing configuration key {data_factory_key}")
        lang_dict = get_supported_languages(
            lang_file=lang_file,
            data_access=lang_data_access_factory.create_data_access(),
            logger=self.logger,
        )
        lang_refs = ray.put(lang_dict)
        self.logger.debug(f"Placed language list into Ray object storage under reference{lang_refs}")
        return {supported_languages_ref_key: lang_refs} | self.params


class CodeToParquetRayConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=CodeToParquetTransformConfiguration(transform_class=CodeToParquetRayTransform),
            runtime_class=CodeToParquetRuntime,
        )


if __name__ == "__main__":
    launcher = RayTransformLauncher(CodeToParquetRayConfiguration())
    launcher.launch()
