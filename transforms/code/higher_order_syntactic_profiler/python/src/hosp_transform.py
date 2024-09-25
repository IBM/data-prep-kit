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

import time
from argparse import ArgumentParser, Namespace
from typing import Any
import csv

import pyarrow as pa
import pyarrow.csv as pacsv
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider


short_name = "sp"
cli_prefix = f"{short_name}_"
# sleep_key = "sleep_sec"
# pwd_key = "pwd"
ikb_file = "ikb_file"
null_libs_file = "null_libs_file"

# sleep_cli_param = f"{cli_prefix}{sleep_key}"
# pwd_cli_param = f"{cli_prefix}{pwd_key}"
ikb_file_cli_param = f"{cli_prefix}{ikb_file}"
null_libs_file_cli_param = f"{cli_prefix}{null_libs_file}"



def process_row(library, language, category, trie):
    base_library_name = library
    trie.insert(str.lower(base_library_name), language, category)

def build_knowledge_base_trie(knowledge_base_table):
    trie = Trie()
    library_column = knowledge_base_table.column('Library').to_pylist()
    language_column = knowledge_base_table.column('Language').to_pylist()
    category_column = knowledge_base_table.column('Category').to_pylist()
    for library, language, category in zip(library_column, language_column, category_column):
        process_row(library, language, category, trie)
    return trie


class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.data = None

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, library_name, programming_language, functionality):
        node = self.root
        for char in library_name:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.data = {}
        node.data['Category'] = functionality
        node.data['Language'] = programming_language
        node.is_end_of_word = True

    def search(self, library_name, programming_language):
        node = self.root
        for char in library_name:
            if char not in node.children:
                return None 
            node = node.children[char]
        if node.is_end_of_word and node.data:
            return node.data
        return None


class knowledge_base:
    knowledge_base_files = []
    knowledge_base_table = None
    null_file = ''
    knowledge_base_trie = None
    entries_with_null_coverage = set()

    def __init__(self, ikb_file, null_libs_file):
        self.knowledge_base_file = ikb_file
        self.null_file = null_libs_file 

    def load_ikb_trie(self):
        self.knowledge_base_table = pacsv.read_csv(self.knowledge_base_file)
        self.knowledge_base_trie = build_knowledge_base_trie(self.knowledge_base_table)

    def write_null_files(self):
        with open(self.null_file, 'a+', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for entry in self.entries_with_null_coverage:
                writer.writerow([entry[0], entry[1]])
        self.entries_with_null_coverage = set()



def concept_extractor(libraries,language,ikb):
    concept_coverage = set()
    language = language
    libraries = [item.strip() for item in libraries.split(",")]
    for library in libraries:
        if library:
            extracted_base_name = str.lower(library)
            matched_entry = ikb.knowledge_base_trie.search(extracted_base_name, language)
            if matched_entry:
                concept_coverage.add(matched_entry['Category'].strip())
            else:
                ikb.entries_with_null_coverage.add((library,language))
    return ','.join(sorted(list(concept_coverage)))



class SemanticProfilerTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, SemanticProfilerTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of SemanticProfilerTransformConfiguration class
        super().__init__(config)
        # self.sleep = config.get("sleep_sec", 1)
        self.ikb_file = config.get("ikb_file", "../src/ikb/ikb_model.csv")
        self.null_libs_file = config.get("null_libs_file", "../src/ikb/null_libs.csv")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows")

        ikb = knowledge_base(self.ikb_file, self.null_libs_file)
        ikb.load_ikb_trie()

        libraries = table.column('Library').to_pylist()
        language = table.column('Language').to_pylist()
        concepts = [concept_extractor(lib, lang, ikb) for lib, lang in zip(libraries, language)]
        new_col = pa.array(concepts)
        table = table.append_column('Concepts', new_col)
        ikb.write_null_files()

        # if self.sleep is not None:
        #     self.logger.info(f"Sleep for {self.sleep} seconds")
        #     time.sleep(self.sleep)
        #     self.logger.info("Sleep completed - continue")
        # Add some sample metadata.
        self.logger.debug(f"Transformed one table with {len(table)} rows")
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata


class SemanticProfilerTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=SemanticProfilerTransform,
            # remove_from_metadata=[pwd_key],
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the SemanticProfilerTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, sp_, pii_, etc.)
        """
        # parser.add_argument(
        #     f"--{sleep_cli_param}",
        #     type=int,
        #     default=1,
        #     help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        # )
        # # An example of a command line option that we don't want included
        # # in the metadata collected by the Ray orchestrator
        # # See below for remove_from_metadata addition so that it is not reported.
        # parser.add_argument(
        #     f"--{pwd_cli_param}",
        #     type=str,
        #     default="nothing",
        #     help="A dummy password which should be filtered out of the metadata",
        # )

        parser.add_argument(
            f"--{ikb_file_cli_param}",
            type=str,
            default="ikb/ikb_model.csv",
            help="Default IKB file",
        )

        parser.add_argument(
            f"--{null_libs_file_cli_param}",
            type=str,
            default="ikb/null_libs.csv",
            help="Default Null Libraries file",
        )


    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        # if captured.get(sleep_key) < 0:
        #     print(f"Parameter sp_sleep_sec should be non-negative. you specified {args.sp_sleep_sec}")
        #     return False

        self.params = self.params | captured
        self.logger.info(f"sp parameters are : {self.params}")
        return True
