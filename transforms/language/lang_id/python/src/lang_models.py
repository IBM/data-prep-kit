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

import math
from abc import ABCMeta, abstractmethod

import fasttext
from huggingface_hub import hf_hub_download
from langcodes import standardize_tag


KIND_FASTTEXT = "fasttext"


class LangModel(metaclass=ABCMeta):
    @abstractmethod
    def detect_lang(self, text: str) -> tuple[str, float]:
        pass


class NoopModel(metaclass=ABCMeta):
    def detect_lang(self, text: str) -> tuple[str, float]:
        return "en", 0.0


class FastTextModel(LangModel):
    def __init__(self, url, credential):
        model_path = hf_hub_download(repo_id=url, filename="model.bin", token=credential)
        self.nlp = fasttext.load_model(model_path)

    def detect_lang(self, text: str) -> tuple[str, float]:
        label, score = self.nlp.predict(
            text.replace("\n", " "), 1
        )  # replace newline to avoid ERROR: predict processes one line at a time (remove '\n') skipping the file
        return standardize_tag(label[0].replace("__label__", "")), math.floor(score[0] * 1000) / 1000


class LangModelFactory:
    def create_model(kind: str, url: str, credential: str) -> LangModel:
        if kind == KIND_FASTTEXT:
            return FastTextModel(url, credential)
        else:
            return NoopModel()
