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
from data_processing.utils import get_logger


logger = get_logger(__name__)

def import_class(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


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
    def __init__(self):
        """
        Initialization - building transforms dictionary
        """
        self.transforms = {
            "code2parquet": ("transforms/code/code2parquet/",       # Subdirectory in the project
                             "dpk_code2parquet_transform_python",   # Python library name
                             "dpk_code2parquet_transform_ray",      # Ray library name
                             None,                                  # Spark library name
                             "code2parquet_transform_python.CodeToParquetPythonConfiguration",  # Python configuration class
                             "code2parquet_transform_ray.CodeToParquetRayConfiguration",  # Ray configuration class
                             None),  # Spark configuration class
            "code_quality": ("transforms/code/code_quality/", "dpk_code_quality_transform_python",
                             "dpk_code_quality_transform_ray", None,
                             "code_quality_transform.CodeQualityTransformConfiguration",
                             "code_quality_transform_ray.CodeQualityRayTransformConfiguration", None),
            "malware": ("transforms/code/malware/", "dpk_malware_transform_python", "dpk_malware_transform_ray", None,
                        "malware_transform_python.MalwarePythonTransformConfiguration",
                        "malware_transform_ray.MalwareRayTransformConfiguration", None),
            "proglang_select": ("transforms/code/proglang_select/", "dpk_proglang_select_transform_python",
                                "dpk_proglang_select_transform_ray", None,
                                "proglang_select_transform_python.ProgLangSelectPythonConfiguration",
                                "proglang_select_transform_ray.ProgLangSelectRayConfiguration", None),
            "lang_id": ("transforms/language/lang_id/", "dpk_lang_id_transform_python", "dpk_lang_id_transform_ray",
                        None, "lang_id_transform_python.LangIdentificationPythonTransformConfiguration",
                        "lang_id_transform_ray.LangIdentificationRayTransformConfiguration", None),
            "doc_id": ("transforms/universal/doc_id/", None, "dpk_docid_transform_ray", None, None,
                       "doc_id_transform_ray.DocIDRayTransformConfiguration", None),
            "ededup": ("transforms/universal/ededup/", None, "dpk_ededup_transform_ray", None, None,
                       "ededup_transform_ray.EdedupRayTransformConfiguration", None),
            "fdedup": ("transforms/universal/fdedup/", None, "dpk_fdedup_transform_ray", None, None,
                       "fdedup_transform_ray.FdedupRayTransformConfiguration", None),
            "filter": ("transforms/universal/filter/", "dpk_filter_transform_python", "dpk_filter_transform_ray", None,
                       "filter_transform_python.FilterPythonTransformConfiguration",
                       "filter_transform_ray.FilterRayTransformConfiguration", None),
            "noop": ("transforms/universal/noop/", "dpk_noop_transform_python", "dpk_noop_transform_ray", None,
                     "noop_transform_python.NOOPPythonTransformConfiguration",
                     "noop_transform_ray.NOOPRayTransformConfiguration", None),
            "profiler": ("transforms/universal/profiler/", None, "dpk_profiler_transform_ray", None, None,
                         "profiler_transform_ray.ProfilerRayTransformConfiguration", None),
            "resize": ("transforms/universal/resize/", "dpk_resize_transform_python", "dpk_resize_transform_ray", None,
                       "resize_transform_python.ResizePythonTransformConfiguration",
                       "resize_transform_ray.ResizeRayTransformConfiguration", None),
            "tokenization": ("transforms/universal/tokenization/", "dpk_tokenization_transform_python",
                             "dpk_tokenization_transform_ray", None,
                             "tokenization_transform_python.TokenizationPythonConfiguration",
                             "tokenization_transform_ray.TokenizationRayConfiguration", None),
        }

    def get_configuration(self, transform: str,
                          runtime: TransformRuntime = TransformRuntime.PYTHON) -> tuple[str, str, str]:
        """
        Get configuration for a given transform/runtime
        :param transform: transform name
        :param runtime: runtime
        :return: a tuple containing transform subdirectory, library name and configuration class name.
                 Return None if transform/runtime does not exist
        """
        config = self.transforms.get(transform, None)
        if config is None:
            logger.warning(f"transform {transform} is not defined")
            return None, None, None
        match runtime:
            case TransformRuntime.PYTHON:
                # runtime Python
                if config[1] is None or config[4] is None:
                    logger.warning(f"transform {transform} for Python is not defined")
                    return None, None, None
                return config[0] + "python", config[1], config[4]
            case TransformRuntime.RAY:
                # runtime Ray
                if config[2] is None or config[5] is None:
                    logger.warning(f"transform {transform} for Ray is not defined")
                    return None, None, None
                return config[0] + "ray", config[2], config[5]
            case TransformRuntime.SPARK:
                # runtime Spark
                if config[3] is None or config[6] is None:
                    logger.warning(f"transform {transform} for Spark is not defined")
                    return None, None, None
                return config[0] + "spark", config[3], config[6]
            case _:
                logger.warning(f"undefined runtime {runtime.name}")
                return None, None, None
