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

from code2parquet_transform import (
    CodeToParquetTransform,
    CodeToParquetTransformConfiguration,
    data_factory_key,
    get_supported_languages,
    supported_langs_file_key,
)
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.utils import get_logger


logger = get_logger(__name__)


class CodeToParquetPythonTransform(CodeToParquetTransform):
    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, LangSelectorTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        super().__init__(config)
        path = config.get(supported_langs_file_key, None)
        if path is None:
            raise RuntimeError(f"Missing configuration value for key {supported_langs_file_key}")
        daf = config.get(data_factory_key, None)
        data_access = daf.create_data_access()
        self.languages_supported = get_supported_languages(lang_file=path, data_access=data_access, logger=self.logger)


class CodeToParquetPythonConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=CodeToParquetTransformConfiguration(transform_class=CodeToParquetPythonTransform)
        )


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = PythonTransformLauncher(CodeToParquetPythonConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
