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

from abc import ABCMeta, abstractmethod
from data_processing.utils import get_logger, TransformUtils
import os
import pyarrow as pa
import sys

logger = get_logger(__name__)

class PerplexityModel(metaclass=ABCMeta):
    @abstractmethod
    def get_perplexities_raw(self, texts: pa.ChunkedArray) -> list:
        raise NotImplementedError

    def get_perplexities(self, texts: pa.ChunkedArray, digit: int) -> list:
        return map(lambda score: round(score, digit), self.get_perplexities_raw(texts))

class NoopModel(PerplexityModel):
    def get_perplexities_raw(self, texts: pa.ChunkedArray) -> list:
        return [0.0] * texts.length()

class PerplexityModelFactory:
    @staticmethod
    def create_model(model_class_name: str, model_path: str) -> PerplexityModel:
        import importlib, pkgutil
        for path in sys.path:
            # Iterate through modules in the path
            for importer, module_name, ispkg in pkgutil.iter_modules([path]):
                full_module_name = f"{module_name}"
                
                # Import the module dynamically
                try:
                    module = importlib.import_module(full_module_name)
                    # Check if the class exists in the module
                    if hasattr(module, model_class_name):
                        print(f"Found class {model_class_name} in module: {full_module_name}")
                        found_class = getattr(__import__(full_module_name), model_class_name)
                        return found_class(model_path)
                except Exception as e:
                    print(f"Error importing module {full_module_name}: {e}")
        raise Exception(f"Could not find the class with name {model_class_name}")