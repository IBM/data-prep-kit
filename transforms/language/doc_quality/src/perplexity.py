"""
@ Create on: 2023/04/25
@ Description:
    To compute perplexity (quality score) for documents.

@ Require installation of KenLM and SentencePiece as follows:
    pip install https://github.com/kpu/kenlm/archive/master.zip
    pip install sentencepiece

@ Download pre-trained KenLM model for English language on Wikipedia
    through cc_net (https://github.com/facebookresearch/cc_net):
        make lang=en lm
  Save 2 downloaded files (en.arpa.bin, en.sp.model) to local machine
  as its size is > 4GB.
  Or access them in COS at:
    s3://cos-optimal-llm-pile/bluepile-processing/lm_sp/

@ To train KenLM on other languages or on text corpus other than Wikipedia (e.g, OSCAR)
  please refer to this HF repo: https://github.com/bigscience-workshop/data_tooling/tree/master/kenlm_training
  (related to HF's ROOTS/BLOOM project https://arxiv.org/pdf/2303.03915.pdf). This project has also provided pre-trained models trained on Wikipedia and OSCAR datasets.
  They can be directly downloaded from: https://huggingface.co/edugp/kenlm/tree/main/
  in two corresponding folders `wikipedia` or `oscar` folder.
"""

import logging


logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("perplexity")

import os

import kenlm
import sentencepiece
from cc_net_prepro import cc_net_normalize


# Ensure following path to folder having pre-trained KenLM + SentencePiece (sp):
PATH_TO_PRETRAINED_MODELS = "/dev/cos/bluepile-processing/lm_sp/"  # on a VM
# PATH_TO_PRETRAINED_MODELS = '~/BigData/04-FM/lm_sp/'  # on a MAC
TEST_MODE = 0


class SentencePieceModel:
    """
    Tokenize a given text into tokens/subwords and then join them together with whitespace between tokens.
    Examples:
        For `en` text:
            mmodel_path = os.path.join(os.path.expanduser('~/BigData/04-FM/lm_sp/'), "en.sp.model")
            spm = SentencePieceModel(model_path)
            out_text = spm.tokenize(text="Café élevàtor ôperàtor naïve Noël façade don't",
                                    strip_accent=True, # set to False for comparison
                                    lower_case=True,
                                    digit_2_zero=True,
                                    punct_level=1,
                                    language='en',
                                    verbose=True)
            >>>
                == input text: Café élevàtor ôperàtor naïve Noël façade don't
                == normalized text: Café élevàtor ôperàtor naïve Noël façade don't
                -> tokenized to 9 tokens: ['▁cafe', '▁elevator', '▁operator', '▁naive', '▁noel', '▁facade', '▁don', "'", 't']
                -> output text: ▁cafe ▁elevator ▁operator ▁naive ▁noel ▁facade ▁don ' t

            Compared to w/o Unicode normalization:
                -> tokenized to 15 tokens: ['▁cafe', '▁elevator', '▁operator', '▁na', 'ï', 've', '▁no', 'ë', 'l', '▁fa', 'ç', 'ade', '▁don', "'", 't']
                -> output text: ▁cafe ▁elevator ▁operator ▁na ï ve ▁no ë l ▁fa ç ade ▁don ' t

        For `ja` text (trained by IBM TRL team):
            model_path = os.path.join(os.path.expanduser('~/BigData/04-FM/lm_sp/'), "ja.sp.model")
            spm = SentencePieceModel(model_path)
            out_text = spm.tokenize(text='これは机です。あれは鉛筆です。',
                                    strip_accent=True,
                                    lower_case=False,
                                    digit_2_zero=True,
                                    punct_level=1,
                                    language='ja',
                                    verbose=True)
            >>>
                == input text: これは机です。あれは鉛筆です。
                == normalized text: これは机です。あれは鉛筆です。
                -> tokenized to 10 tokens: ['▁', 'これは', '机', 'です', '.', 'あれ', 'は', '鉛筆', 'です', '.']
                -> output text: ▁ これは 机 です . あれ は 鉛筆 です .

        For same text but using `ja_meta` (trained by Meta):
            model_path = os.path.join(os.path.expanduser('~/BigData/04-FM/lm_sp/'), "ja_meta.sp.model")
            spm = SentencePieceModel(model_path)
            out_text = spm.tokenize(text='これは机です。あれは鉛筆です。',
                                    strip_accent=False,
                                    lower_case=False,
                                    digit_2_zero=True,
                                    punct_level=1,
                                    language='ja',
                                    verbose=True)
            >>>
                == input text: これは机です。あれは鉛筆です。
                == normalized text: これは机です。あれは鉛筆です。
                -> tokenized to 12 tokens: ['▁', 'これは', '机', 'で', 'す', '.', 'あれ', 'は', '鉛筆', 'で', 'す', '.']
                -> output text: ▁ これは 机 で す . あれ は 鉛筆 で す .
    """

    def __init__(self, pre_trained_model: str):
        super().__init__()
        try:
            self.pre_trained_model = pre_trained_model
            self.sp = sentencepiece.SentencePieceProcessor()
            self.sp.load(self.pre_trained_model)
            logger.info(f"== PRE-TRAINED SENTENCE PIECE: {self.pre_trained_model}")
        except Exception as e:
            raise Exception(f"== Failed in loading {pre_trained_model} due to: {e}")

    def tokenize(
        self,
        text: str,
        strip_accent: bool,
        lower_case: bool,
        digit_2_zero: bool,
        punct_level: int,
        language: str,
        verbose=False,
    ) -> str:
        """
        Tokenize given text into tokens then join them with whitespace as separation into
        a single out_text to return.
        """

        # Normalize `text` prior to tokenizing it into tokens
        text_normalized = cc_net_normalize(
            text,
            strip_accent=strip_accent,
            lower_case=lower_case,
            digit_2_zero=digit_2_zero,
            punct_level=punct_level,
            language=language,
        )

        # Conduct tokenization
        tokenized = self.sp.encode_as_pieces(text_normalized)
        out_text = " ".join(tokenized)
        if verbose:
            print(f"==== sentence piece model: {self.pre_trained_model} ====\n")
            print(f"== input text: {text}")
            print(f"== normalized text: {text}")
            print(f"-> tokenized to {len(tokenized)} tokens: {tokenized}")
            print(f"-> output text: {out_text}")
        return out_text


class KenLMModel:
    """
    Load a pre-trained KenLM and its corresponding SentencePiece tokenizer based on a given language
    to compute perplexity of a given text. The given text can be `normalized` by cc_net_normalize().
    """

    def __init__(
        self,
        model_path: str,
        language: str = "en",
        strip_accent: bool = True,
        lower_case: bool = True,
        digit_2_zero: bool = True,
        punct_level: int = 1,
    ):
        logger.info(f"== PATH TO PRETRAINED KenLM and SENTENCEPIECE: {model_path}")
        logger.info(f"== LANGUAGE: {language}")
        logger.info(f"== STRIP ACCENT: {strip_accent}")
        logger.info(f"== CONVERT TO LOWER CASE: {lower_case}")
        logger.info(f"== CONVERT DIGITS TO ZERO: {digit_2_zero}")
        logger.info(f"== LEVEL OF REPLACING PUNCTUATION: {punct_level}")

        """Load pre-trained KenLM and sp: """
        try:
            self.kenlm_model = kenlm.Model(os.path.join(model_path, f"{language}.arpa.bin"))
            self.sp_tokenizer = SentencePieceModel(os.path.join(model_path, f"{language}.sp.model"))
        except Exception as e:
            raise Exception(f"== Failed in loading pre-trained KenLM/SentencePiece of `{language}` due to: {e}")
        self.strip_accent = strip_accent
        self.lower_case = lower_case
        self.digit_2_zero = digit_2_zero
        self.punct_level = punct_level
        self.language = language

    @classmethod
    def from_pretrained(
        cls,
        model_path: str,
        language: str = "en",
        strip_accent: bool = True,
        lower_case: bool = True,
        digit_2_zero: bool = True,
        punct_level: int = 1,
    ):
        return cls(
            model_path,
            language,
            strip_accent,
            lower_case,
            digit_2_zero,
            punct_level,
        )

    def get_perplexity_ver01(self, doc: str) -> float:
        """
        This version normalizes the entire `doc` first which may remove linebreaks
        and make entire document as a paragraph/line.
        """

        # normalization will remove line breaks, making entire doc as a single line.
        doc_norm = cc_net_normalize(
            doc,
            strip_accent=self.strip_accent,
            lower_case=self.lower_case,
            digit_2_zero=self.digit_2_zero,
            punct_level=self.punct_level,
        )  # this may combine all lines/paragraphs into one (i.e., no line breaks)
        """
        Tokenize text after normalizing it.
        Reference to full pipeline:
            https://github.com/facebookresearch/cc_net/blob/bda555bd1cf1ee2e0b925363e62a61cd46c8b60d/cc_net/mine.py#L352
        """
        doc_tokenized = self.sp_tokenizer.tokenize(doc_norm)
        if TEST_MODE:
            print(f"\n== raw doc: {doc}")
            print(f"\n== doc_norm: {doc_norm}")
            print(f"\n== doc_tokenized: {doc_tokenized}")

        doc_log_score, doc_length = 0, 0
        for i, line in enumerate(doc_tokenized.split("\n")):
            if len(line) == 0:
                continue
            doc_log_score += self.kenlm_model.score(line)
            doc_length += len(line.split()) + 1

        pp_score = round(10.0 ** (-doc_log_score / doc_length), 1)
        return pp_score

    def get_perplexity(self, doc: str) -> float:
        """
        This version breaks `doc` into lines and compute score per line
        and aggregate them to a single score of the entire `doc`.

        Parameters
        ----------
        doc: a text document

        Returns
        -------
        perplexity score as non-negative float score. The smaller the score, the more natural
        the language used in `doc`.

        Reference:
            https://github.com/facebookresearch/cc_net/blob/bda555bd1cf1ee2e0b925363e62a61cd46c8b60d/cc_net/mine.py#L352
            (which may differ by normalizing doc first)
        """

        doc_log_score, doc_length = 0, 0
        for i, line in enumerate(doc.split("\n")):
            line_tokenized = self.sp_tokenizer.tokenize(
                line,
                strip_accent=self.strip_accent,
                lower_case=self.lower_case,
                digit_2_zero=self.digit_2_zero,
                punct_level=self.punct_level,
                language=self.language,
            )

            if len(line_tokenized) == 0:
                continue
            log_score = self.kenlm_model.score(line_tokenized)
            length = len(line_tokenized.split()) + 1  # no. of tokens
            doc_log_score += log_score
            doc_length += length
            if TEST_MODE:
                print(
                    f"\n== {i:3d} length: {length} log_score: {log_score:.1f}\n + Original line: {line}\n + line_tokenized: {line_tokenized}"
                )
        if doc_length > 0:
            pp_score = round(10.0 ** (-doc_log_score / doc_length), 1)
        else:
            pp_score = float("inf")
        if TEST_MODE:
            print(
                f"\n=== doc_log_score: {doc_log_score:.1f} doc_length: {doc_length} -doc_log_score/doc_length:{(-doc_log_score/doc_length):.1f} pp_score:{pp_score:.1f}"
            )
        return pp_score
