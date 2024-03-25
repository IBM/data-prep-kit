import argparse
from argparse import ArgumentParser
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import get_logger
from dotenv import load_dotenv


logger = get_logger(__name__)


# BAD_WORD_DIR = "cos-optimal-llm-pile/bluepile-processing/docq/ldnoobw/"
# BAD_WORD_DIR = "~/Desktop/GUF_hajar/fm-data-engineering/transforms/language/doc_quality/test-data/docq/ldnoobw/"   #local

# MODEL_DIR = "cos-optimal-llm-pile/bluepile-processing/lm_sp/"
# MODEL_DIR = "../lm_sp/"       #local


class DocQualityTransform(AbstractTableTransform):
    """
    Implements various docuement quality metrics to documents in a pyarrow Table.
    """

    def __init__(self, config: dict):
        """
        This class is used to transform an input table to an output table utilizing a doc quality annotator.
        The input table must contain at least two columns, with default names set as `document_id` and `contents`.
        The doc quality transformer will add document quality metrics to the input table.
        """
        from doc_c4_statistics import c4_load_ldnoobw_words
        from perplexity import KenLMModel

        super().__init__(config)
        self.warning_issued = config.get("warning_issued", False)
        self.ft_lang = config.get("ft_lang", "en")
        self.bad_word_filepath = config.get(
            "bad_word_filepath" + self.ft_lang,
            "~/Desktop/GUF_hajar/fm-data-engineering/transforms/language/doc_quality/test-data/docq/ldnoobw/en",
        )
        self.col_name = config.get("col_name", "contents")

        self.re_pattern = c4_load_ldnoobw_words(ft_lang=self.ft_lang, file_path=self.bad_word_filepath)
        self.MODEL_DIR = config.get("MODEL_DIR", "../lm_sp/")
        strip_accent = True
        self.klm = KenLMModel.from_pretrained(
            model_path=self.MODEL_DIR, language=self.ft_lang, strip_accent=strip_accent
        )

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:

        from doc_c4_statistics import (
            c4_contain_pattern_ratio,
            c4_contains_ldnoobw_words,
            c4_sentence_count,
        )
        from doc_Gopher_statistics import (
            compute_average_japanese_sentence_length,
            compute_bullet_point_ellipsis_alphabet_word_ratio,
            compute_word_statistics,
            contains_common_English_words,
            find_first_japanese_alphabet_position,
        )

        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.
        """
        new_columns = [
            "docq_total_words",
            "docq_mean_word_len",
            "docq_symbol_to_word_ratio",
            "docq_sentence_count",
            "docq_lorem_ipsum_ratio",
            "docq_curly_bracket_ratio",
            "docq_contain_bad_word",
            "docq_avg_ja_sentence_len",
            "docq_first_ja_alphabet_pos",
            "metakenlm_docq_perplex_score",
            "docq_contain_common_en_words",
            "docq_bullet_point_ratio",
            "docq_ellipsis_line_ratio",
            "docq_alphabet_word_ratio",
        ]

        docq_total_words = []
        docq_mean_word_len = []
        docq_symbol_to_word_ratio = []
        docq_sentence_count = []
        docq_curly_bracket_ratio = []
        docq_lorem_ipsum_ratio = []
        docq_contain_bad_word = []
        docq_bullet_point_ratio = []
        docq_ellipsis_line_ratio = []
        docq_alphabet_word_ratio = []
        docq_contain_common_en_words = []
        docq_perplex_score = []
        if self.ft_lang == "ja":
            # for japanese language, add 2 extra columns for 2 heuristic rules:
            docq_avg_ja_sentence_len = []
            docq_first_ja_alphabet_pos = []

        for text in table[self.col_name].to_pylist():
            total_words, mean_word_len, symbol_to_word_ratio = compute_word_statistics(text)
            docq_total_words.append(total_words)
            docq_mean_word_len.append(mean_word_len)
            docq_symbol_to_word_ratio.append(symbol_to_word_ratio)

            docq_sentence_count.append(c4_sentence_count(text, ft_lang=self.ft_lang))

            docq_lorem_ipsum_ratio.append(
                c4_contain_pattern_ratio(text, pattern="lorem ipsum", ft_lang=self.ft_lang, normalize_text=True)
            )
            curly_bracket_ratio = 0.0
            for sign in ["{", "}"]:
                curly_bracket_ratio += c4_contain_pattern_ratio(
                    text, pattern=sign, ft_lang=self.ft_lang, normalize_text=False
                )
            docq_curly_bracket_ratio.append(curly_bracket_ratio)
            docq_contain_bad_word.append(c4_contains_ldnoobw_words(text, self.re_pattern))

            (
                bullet_point_ratio,
                ellipsis_line_ratio,
                alphabet_word_ratio,
            ) = compute_bullet_point_ellipsis_alphabet_word_ratio(text)
            docq_bullet_point_ratio.append(bullet_point_ratio)
            docq_ellipsis_line_ratio.append(ellipsis_line_ratio)
            docq_alphabet_word_ratio.append(alphabet_word_ratio)

            docq_contain_common_en_words.append(contains_common_English_words(text, self.ft_lang))

            docq_perplex_score.append(self.klm.get_perplexity(text))

            if self.ft_lang == "ja":
                docq_avg_ja_sentence_len.append(compute_average_japanese_sentence_length(text))
                docq_first_ja_alphabet_pos.append(find_first_japanese_alphabet_position(text))

        table = table.append_column("docq_total_words", pa.array(docq_total_words))
        table = table.append_column("docq_mean_word_len", pa.array(docq_mean_word_len))
        table = table.append_column("docq_symbol_to_word_ratio", pa.array(docq_symbol_to_word_ratio))
        table = table.append_column("docq_sentence_count", pa.array(docq_sentence_count))
        table = table.append_column("docq_lorem_ipsum_ratio", pa.array(docq_lorem_ipsum_ratio))
        table = table.append_column("docq_curly_bracket_ratio", pa.array(docq_curly_bracket_ratio))
        table = table.append_column("docq_contain_bad_word", pa.array(docq_contain_bad_word))
        table = table.append_column("docq_bullet_point_ratio", pa.array(docq_bullet_point_ratio))
        table = table.append_column("docq_ellipsis_line_ratio", pa.array(docq_ellipsis_line_ratio))
        table = table.append_column("docq_alphabet_word_ratio", pa.array(docq_alphabet_word_ratio))
        table = table.append_column("docq_contain_common_en_words", pa.array(docq_contain_common_en_words))
        table = table.append_column("metakenlm_docq_perplex_score", pa.array(docq_perplex_score))

        if self.ft_lang == "ja":
            table = table.append_column("docq_avg_ja_sentence_len", pa.array(docq_avg_ja_sentence_len))
            table = table.append_column("docq_first_ja_alphabet_pos", pa.array(docq_first_ja_alphabet_pos))

        metadata = {
            "total_docs_count": table.num_rows,
        }

        return [table], metadata


class DocQualityTransformConfiguration(DefaultTableTransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="DocQuality", transform_class=DocQualityTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, docq_, pii_, etc.)
        """
        parser.add_argument(
            "--ft_lang",
            default="en",
        )
        parser.add_argument(
            "--col_name",
            default="contents",
            help="column name that contain document text",
        )
        parser.add_argument(
            "--bad_word_filepath",
            default="../test-data/docq/ldnoobw/",
            help="path to bad word file",
        )
        parser.add_argument(
            "--MODEL_DIR",
            default="../lm_sp/",
            help="path to model",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        self.params["ft_lang"] = args.ft_lang
        self.params["col_name"] = args.col_name
        self.params["bad_word_filepath"] = args.bad_word_filepath
        self.params["MODEL_DIR"] = args.MODEL_DIR
        return True

    def get_transform_metadata(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        return self.params


if __name__ == "__main__":
    # create launcher
    launcher = TransformLauncher(transform_runtime_config=DocQualityTransformConfiguration())
    logger.info("Launching Doc Quality transform")
    # launch
    launcher.launch()
