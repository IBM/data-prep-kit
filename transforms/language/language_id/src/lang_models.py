from abc import ABCMeta, abstractmethod

# import pyizumo
import fasttext
from huggingface_hub import hf_hub_download
from langcodes import standardize_tag


KIND_FASTTEXT = "fasttext"


class LangModel(metaclass=ABCMeta):
    @abstractmethod
    def detect_lang(self, text: str) -> str:
        pass


class WatsonNLPModel(LangModel):
    def __init__(self):
        pass

    #        self.nlp = pyizumo.load(parsers=["langdetect"])

    def detect_lang(self, text: str) -> str:
        return "en"


#       return self.nlp(text).locale


class FastTextModel(LangModel):
    def __init__(self, url, credential):
        model_path = hf_hub_download(repo_id=url, filename="model.bin", token=credential)
        self.nlp = fasttext.load_model(model_path)

    def detect_lang(self, text: str) -> str:
        label, score = self.nlp.predict(
            text.replace("\n", " "), 1
        )  # replace newline to avoid ERROR: predict processes one line at a time (remove '\n') skipping the file
        return standardize_tag(label[0].replace("__label__", ""))


class LangModelFactory:
    def create_model(kind: str, url: str, credential: str) -> LangModel:
        if kind == KIND_FASTTEXT:
            return FastTextModel(url, credential)
        else:
            return WatsonNLPModel()
