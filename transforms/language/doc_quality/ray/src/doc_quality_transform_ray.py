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
from doc_quality_transform import DocQualityTransformConfiguration


logger = get_logger(__name__)

short_name = "docq"
cli_prefix = f"{short_name}_"
text_lang_key = "text_lang"
doc_content_column_key = "doc_content_column"
doc_id_column_key = "doc_id_column"
bad_word_filepath_key = "bad_word_filepath"
kenLM_model_key = "kenLM_model"
text_lang_cli_param = f"{cli_prefix}{text_lang_key}"
doc_content_column_cli_param = f"{cli_prefix}{doc_content_column_key}"
doc_id_column_cli_param = f"{cli_prefix}{doc_id_column_key}"
bad_word_filepath_cli_param = f"{cli_prefix}{bad_word_filepath_key}"
kenLM_model_cli_param = f"{cli_prefix}{kenLM_model_key}"


class DocQualityRayTransformConfiguration(RayTransformRuntimeConfiguration):
    """
    Implements the RayTransformConfiguration for Document Quality as required by the RayTransformLauncher.
    Document Quality does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """
    
    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(transform_config=DocQualityTransformConfiguration())

if __name__ == "__main__":
    launcher = RayTransformLauncher(DocQualityRayTransformConfiguration())
    logger.info("Launching doc_quality transform")
    launcher.launch()