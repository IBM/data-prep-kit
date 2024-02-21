import argparse
import time
from argparse import ArgumentParser
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    AbstractTableTransformRuntimeFactory,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform

'''
Reference https://github.ibm.com/ai-foundation/foundation-model-stack/blob/main/preprocessing/ray/language_identification_sentence_split/liss_actor.py
'''
class LangIdentificationTransform(AbstractTableTransform):
    """
    Implements language identification using IBM pyizumo to documents in a pyarrow Table.
    """
    import pyizumo
    from transforms.language.language_id.watson_nlp import get_lang_ds_pa

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, NOOPTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        super().__init__(config)
        self.warning_issued = False
        self.nlp_langid = pyizumo.load(parsers=["langdetect"])
        if "drop_column_if_existed" in config:
            self.drop_column_if_existed = config["drop_column_if_existed"]
        else:
            self.drop_column_if_existed = True


    def transform(self, table: pa.Table) -> list[pa.Table]:
        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.
        """
        new_columns = ["ft_lang", "ft_score"]
        for column in new_columns:
            if column in table.column_names:
                if self.drop_column_if_existed:
                    if not self.warning_issued:
                        # print(f"WARNING: drop existing column {column}. {input_parquet_path}")
                        print(f"WARNING: drop existing column {column}")
                        self.warning_issued = True
                    table = table.drop(column)
                else:
                    print(
                        f"ERROR: existing column {column} found and drop_column_if_existed is false. "
                        f"Terminating..."
                    )
                    exit(-1)

        table_langid, stats = get_lang_ds_pa(table, self.nlp_langid, col_name="contents")

        # Need to return stats and customize metadata output
        return [table_langid]


class LangIdentificationTransformRuntimeFactory(AbstractTableTransformRuntimeFactory):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(runtime_class=DefaultTableTransformRuntime,
                         transformer_class=LangIdentificationTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument("-dr", "--drop_column_if_existed", default=False, help="drop columns if existed")

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments including at least, but perhaps more,
        arguments as defined by add_input_arguments().
        :return: True, if validate pass or False otherwise
        """
        self.params["drop_column_if_existed"] = args.drop_column_if_existed
        return True

    def get_input_params_metadata(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return:
        """
        return self.params


if __name__ == "__main__":
    # create launcher
    launcher = TransformLauncher(name="LangIdentificationTransform",
                                 transform_runtime_factory=LangIdentificationTransformRuntimeFactory())
    # create parameters

    # launch
    launcher.launch()
