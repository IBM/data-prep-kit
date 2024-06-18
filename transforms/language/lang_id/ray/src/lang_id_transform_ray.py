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

import pyarrow as pa
from data_processing.utils import get_logger
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from lang_id_transform import LangIdentificationTransformConfiguration


logger = get_logger(__name__)

short_name = "lang_id"
cli_prefix = f"{short_name}_"
model_credential_key = "model_credential"
model_kind_key = "model_kind"
model_url_key = "model_url"
content_column_name_key = "content_column_name"
model_credential_cli_param = f"{cli_prefix}{model_credential_key}"
model_kind_cli_param = f"{cli_prefix}{model_kind_key}"
model_url_cli_param = f"{cli_prefix}{model_url_key}"
content_column_name_cli_param = f"{cli_prefix}{content_column_name_key}"


class LangIdentificationRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for Language Identification as required by the RayTransformLauncher.
    Language Identification does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(transform_config=LangIdentificationTransformConfiguration())


if __name__ == "__main__":
    launcher = RayTransformLauncher(LangIdentificationRayTransformConfiguration())
    logger.info("Launching lang_id transform")
    launcher.launch()
