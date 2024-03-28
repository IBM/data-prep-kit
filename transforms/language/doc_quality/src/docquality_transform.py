import argparse
from argparse import ArgumentParser

import pyarrow as pa
from data_processing.ray import DefaultTableTransformConfiguration, TransformLauncher
from data_processing.transform import AbstractTableTransform
from data_processing.utils import TransformUtils, get_logger
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


logger = get_logger(__name__)


class DocQualityTransform(AbstractTableTransform):
    """
    Implements various docuement quality metrics to documents in a pyarrow Table.
    """

    def __init__(self, config: dict):
        """
        This class is used to transform an input table to an output table utilizing a doc quality annotator.
        The input table must contain at least two columns, by default named `document_id` and `contents`.
        The doc quality transformer will add document quality metrics to the input table.
        """
        from doc_c4_statistics import c4_load_ldnoobw_words
        from perplexity import KenLMModel

        super().__init__(config)
        self.warning_issued = config.get("warning_issued", False)
        self.docq_text_lang = config.get("docq_text_lang", "en")
        self.bad_word_filepath = config.get(
            "bad_word_filepath" + self.docq_text_lang,
            "../test-data/docq/ldnoobw/en",
        )
        self.docq_doc_content_column = config.get("docq_doc_content_column", "contents")
        self.docq_doc_id_column = config.get("docq_doc_id_column", "document_id")

        self.re_pattern = c4_load_ldnoobw_words(ft_lang=self.docq_text_lang, file_path=self.bad_word_filepath)
        self.docq_kenLM_model = config.get("docq_kenLM_model", "../lm_sp/")
        strip_accent = True
        self.klm = KenLMModel.from_pretrained(
            model_path=self.docq_kenLM_model, language=self.docq_text_lang, strip_accent=strip_accent
        )

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:
        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.
        """
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
        if self.docq_text_lang == "ja":
            # for japanese language, add 2 extra columns for 2 heuristic rules:
            docq_avg_ja_sentence_len = []
            docq_first_ja_alphabet_pos = []

        for text in table[self.docq_doc_content_column].to_pylist():
            total_words, mean_word_len, symbol_to_word_ratio = compute_word_statistics(text)
            docq_total_words.append(total_words)
            docq_mean_word_len.append(mean_word_len)
            docq_symbol_to_word_ratio.append(symbol_to_word_ratio)

            docq_sentence_count.append(c4_sentence_count(text, ft_lang=self.docq_text_lang))

            docq_lorem_ipsum_ratio.append(
                c4_contain_pattern_ratio(text, pattern="lorem ipsum", ft_lang=self.docq_text_lang, normalize_text=True)
            )
            curly_bracket_ratio = 0.0
            for sign in ["{", "}"]:
                curly_bracket_ratio += c4_contain_pattern_ratio(
                    text, pattern=sign, ft_lang=self.docq_text_lang, normalize_text=False
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

            docq_contain_common_en_words.append(contains_common_English_words(text, self.docq_text_lang))

            docq_perplex_score.append(self.klm.get_perplexity(text))

            if self.docq_text_lang == "ja":
                docq_avg_ja_sentence_len.append(compute_average_japanese_sentence_length(text))
                docq_first_ja_alphabet_pos.append(find_first_japanese_alphabet_position(text))

        table = TransformUtils.add_column(table=table, name="docq_total_words", content=docq_total_words)
        table = TransformUtils.add_column(table=table, name="docq_mean_word_len", content=docq_mean_word_len)
        table = TransformUtils.add_column(
            table=table, name="docq_symbol_to_word_ratio", content=docq_symbol_to_word_ratio
        )
        table = TransformUtils.add_column(table=table, name="docq_sentence_count", content=docq_sentence_count)
        table = TransformUtils.add_column(table=table, name="docq_lorem_ipsum_ratio", content=docq_lorem_ipsum_ratio)
        table = TransformUtils.add_column(
            table=table, name="docq_curly_bracket_ratio", content=docq_curly_bracket_ratio
        )
        table = TransformUtils.add_column(table=table, name="docq_contain_bad_word", content=docq_contain_bad_word)
        table = TransformUtils.add_column(table=table, name="docq_bullet_point_ratio", content=docq_bullet_point_ratio)
        table = TransformUtils.add_column(
            table=table, name="docq_ellipsis_line_ratio", content=docq_ellipsis_line_ratio
        )
        table = TransformUtils.add_column(
            table=table, name="docq_alphabet_word_ratio", content=docq_alphabet_word_ratio
        )
        table = TransformUtils.add_column(
            table=table, name="docq_contain_common_en_words", content=docq_contain_common_en_words
        )
        table = TransformUtils.add_column(table=table, name="metakenlm_docq_perplex_score", content=docq_perplex_score)

        if self.docq_text_lang == "ja":
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
            "--docq_text_lang",
            default="en",
        )
        parser.add_argument(
            "--docq_doc_content_column",
            default="contents",
            help="column name that contain document text",
        )
        parser.add_argument(
            "--docq_doc_id_column",
            default="document_id",
            help="column name that contain document id",
        )
        parser.add_argument(
            "--bad_word_filepath",
            default="../test-data/docq/ldnoobw/",
            help="path to bad word file",
        )
        parser.add_argument(
            "--docq_kenLM_model",
            default="../lm_sp/",
            help="path to docq_kenLM_model",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.docq_kenLM_model is None:
            logger.error(
                f"Parameter --docq_kenLM_model must be a valid kenLM model for calculating perplexity, you specified {args.docq_kenLM_model}"
            )
            return False

        if args.bad_word_filepath is None:
            logger.error(
                f"Parameter --bad_word_filepath must be a valid path to bad_word file, you specified {args.bad_word_filepath}"
            )
            return False

        if args.docq_doc_content_column is None:
            logger.error(f"Value for `--docq_doc_content_column` must be provided")
            return False

        # For MVP1: only support english text:
        if args.docq_text_lang != "en":
            logger.error(
                f"This version has not supported languages other than `en` yet, you specified {args.docq_text_lang}"
            )
            return False

        self.params["docq_kenLM_model"] = args.docq_kenLM_model
        self.params["bad_word_filepath"] = args.bad_word_filepath
        self.params["docq_text_lang"] = args.docq_text_lang
        self.params["docq_doc_content_column"] = args.docq_doc_content_column
        self.params["docq_doc_id_column"] = args.docq_doc_id_column
        return True


if __name__ == "__main__":
    # create launcher
    launcher = TransformLauncher(transform_runtime_config=DocQualityTransformConfiguration())
    logger.info("Launching Doc Quality transform")
    # launch
    launcher.launch()
