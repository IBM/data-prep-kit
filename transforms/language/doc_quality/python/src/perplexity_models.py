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
    def create_model(model_module_name: str, model_path: str) -> PerplexityModel:
        import importlib
        model_module_spec = importlib.util.find_spec(model_module_name)
        if model_module_spec is not None:
            model_module = importlib.util.module_from_spec(model_module_spec)
            model_module_spec.loader.exec_module(model_module)
            import inspect
            for name, obj in inspect.getmembers(model_module, inspect.isclass):
                if issubclass(obj, PerplexityModel) and obj is not PerplexityModel:
                    print(f"Found class {name} in module: {model_module.__name__}")
                    return obj(model_path)
        raise Exception(f"Could not find perplexity model class from the module name {model_module_name}")