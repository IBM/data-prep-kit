from abc import ABCMeta, abstractmethod

# import pyizumo
from fasttext import load_model


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
    def __init__(self, path):
        self.nlp = load_model(path)

    def detect_lang(self, text: str) -> str:
        label, score = self.nlp.predict(
            text.replace("\n", " "), 1
        )  # replace newline to avoid ERROR: predict processes one line at a time (remove '\n') skipping the file
        return label[0].replace("__label__", "")


class LangModelFactory:
    def create_model(kind: str, path: str) -> LangModel:
        if kind == KIND_FASTTEXT:
            return FastTextModel(path)
        else:
            return WatsonNLPModel()
