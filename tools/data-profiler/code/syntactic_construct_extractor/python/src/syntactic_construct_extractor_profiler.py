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

import functools
import os
import time
from argparse import ArgumentParser, Namespace
from typing import Any
import numpy as np
import pandas as pd
from pygments.lexers import guess_lexer, get_all_lexers
from pygments.util import ClassNotFound
import pyarrow as pa
import pyarrow.parquet as pq
from langdetect import detect
import requests
from pygments.lexers import guess_lexer, get_all_lexers
from pygments.util import ClassNotFound
from langdetect import detect
from tree_sitter import Language, Parser
from tree_sitter_languages import get_language, get_parser
from pygments import lexers

from data_processing.transform import AbstractTableTransform


class SyntacticConstructExtractorProfiler(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """

        super().__init__(config)
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Extracts the syntactic constructs
        """
        def extract_python_imports(code):
            tree = parser.parse(code.encode())
            root_node = tree.root_node
            packages = set()
            modules = set()

            @functools.lru_cache(maxsize=10000)  # Cache results to avoid repeated checks
            def is_package(module_name):
                """Check if the given module name is a package."""
                try:
                    # Check if already known
                    if module_name in known_packages:
                        return True
                    elif module_name in known_modules:
                        return False

                    if module_name in static_package_set:
                        known_packages.add(module_name)
                        return True

                    known_modules.add(module_name)  # Update non-package cache
                    return False

                except AttributeError:
                    # This occurs if the module has no __path__ attribute, meaning it's not a package
                    return False
                except requests.RequestException as e:
                    #print(f"Network or HTTP request issue: {e}")
                    return False
                except Exception as e:
                    return False

            def handle_import_statement(node):
                # Look for the first dotted_name child node to get the top-level import
                for subchild in node.children:
                    if subchild.type == 'dotted_name':
                        top_level_module = subchild.text.decode('utf8').split('.')[0]
                        print(top_level_module)
                        if top_level_module.startswith(('java.', 'org.')):  # Skip non-Python imports
                            continue

                        # Use the is_package function to check if it's a package or a module
                        if is_package(top_level_module):
                            packages.add(top_level_module)
                        else:
                            modules.add(top_level_module)
                        break  # Exit after processing the first valid dotted_name

            def traverse(node):
                if node.type == 'import_statement' or node.type == 'import_from_statement':
                    print("found import_statement")
                    handle_import_statement(node)
                for child in node.children:
                    traverse(child)

            # Initialize sets
            packages = set()
            modules = set()
            # Assuming you have a root node to start from
            traverse(root_node)  # You will need to parse your code to get the root_node
            print("Packages:", packages)
            print("Modules:", modules)
            return packages, modules
        
        def extract_java_imports(code):
            tree = parser.parse(code.encode())
            root_node = tree.root_node
            packages = set()
            modules = set()

            def handle_import_statement(node):
                # Look for the first dotted_name child node to get the top-level import
                for subchild in node.children:
                    if subchild.type == 'scoped_identifier':
                        top_level_package = subchild.text.decode('utf8')
                        print(top_level_package)
                        # Call is_package and unpack the returned tuple
                        is_pkg, matched_package = search_longest_prefix(trie, top_level_package)
                        print(matched_package)
                        if is_pkg:
                            packages.add(matched_package)  # Always add to packages if true
                            modules.add(matched_package)  # Temporarily add to modules
                        break  # Exit after processing the first valid dotted_name

            def traverse(node):
                if node.type == 'import_declaration':
                    print("found import_statement")
                    handle_import_statement(node)
                for child in node.children:
                    traverse(child)

            # Initialize sets
            packages = set()
            modules = set()
            # Assuming you have a root node to start from
            traverse(root_node)  # You will need to parse your code to get the root_node
            print("Packages:", packages)
            print("Modules:", modules)
            return packages, modules

        def create_trie():
            return {}

        def insert_into_trie( trie, word):
            current_node = trie
            for char in word:
                if char not in current_node:
                    current_node[char] = {}
                current_node = current_node[char]
            # Mark the end of a word
            current_node['#'] = True

        def search_longest_prefix(trie, word):
            current_node = trie
            longest_prefix = ""
            current_prefix = ""
            found = False  # Initialize a flag to indicate if the end of a word has been reached

            for char in word:
                if char in current_node:
                    current_node = current_node[char]
                    current_prefix += char
                    if '#' in current_node:  # Check if this is the end of a word in the trie
                        longest_prefix = current_prefix
                        found = True
                else:
                    break

            return found, longest_prefix


        start_time = time.time()  # Record the start time
        # Convert the pyarrow.Table to a pandas.DataFrame
        df = table.to_pandas()

        # Check if df is a Series, and convert it to a DataFrame if necessary
        if isinstance(df, pd.Series):
            df = df.to_frame()

        # Ensure df is a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected df to be a pandas DataFrame")

        # Print column names for debugging
        print("DataFrame columns:", df.columns)

        if self.config.get("datatype") == "EPT":
            row_alias = "Files"
        elif self.config.get("datatype") == "SFT":
            row_alias = "Instruction Pairs"

        input_code_col = self.config.get("input_code_col")
        output_code_col = self.config.get("output_code_col")

        # Load the Python language grammar
        code_language = self.config.get("code_language")
        TS_CODE_LANGUAGE = get_language(code_language)
        parser = Parser()
        parser.set_language(TS_CODE_LANGUAGE)

        known_packages = set()
        known_modules = set()

        static_package_set = set()
        static_package_set = self.list_static_packages(code_language, static_package_set)

        trie = create_trie()
        if code_language == "java":
            for package in static_package_set:
                insert_into_trie(trie, package)

        package_counts = {}
        module_counts = {}
        package_sizes = {}
        module_sizes = {}
        for index, row in df.iterrows():
            row_packages = set()
            row_modules = set()
            
            for col in [input_code_col, output_code_col]:
                if col in df.columns:
                    if pd.notna(row[col]) and row[col].strip():
                        detected_language = self.detect_prog_lang(df, row, col)
                        if detected_language in ['python', 'numpy'] and code_language == "python":
                            print(f"index: {index}")
                            packages, modules = extract_python_imports(row[col])
                            print(f"packages: {packages}")
                            print(f"modules: {modules}")
                            row_packages.update(packages)
                            row_modules.update(modules)
                            code_size = len(row[col].encode('utf-8'))/1024
                            for package in packages:
                                package_sizes[package] = package_sizes.get(package, 0) + code_size
                            for module in modules:
                                module_sizes[module] = module_sizes.get(module, 0) + code_size
                        if detected_language in ['java'] and code_language == "java":
                            print(f"index: {index}")
                            test_code = """
                            import 
                            import scipy.linalg as np
                            import scipy as sc
                            """
                            packages, modules = extract_java_imports(row[col])
                            print(f"packages: {packages}")
                            row_packages.update(packages)
                            code_size = len(row[col].encode('utf-8'))/1024
                            for package in packages:
                                package_sizes[package] = package_sizes.get(package, 0) + code_size

            for package in row_packages:
                package_counts[package] = package_counts.get(package, 0) + 1

            if code_language == 'python':
                for module in row_modules:
                    module_counts[module] = module_counts.get(module, 0) + 1

        print(f"Number of packages in the {row_alias}:", len(package_counts))
        print(f"{row_alias} per package:", sorted(package_counts.items(), key=lambda x: x[1], reverse=True))
        print(f"Package sizes:", sorted(package_sizes.items(), key=lambda x: x[1], reverse=True))

        metadata = {
            "Data model": "Syntactic construct extractor",
            f"Number of packages in the {row_alias}:": len(package_counts),
            f"{row_alias} per package:": sorted(package_counts.items(), key=lambda x: x[1], reverse=True),            
            f"Size of the code snippets per package (in KB):": sorted(package_sizes.items(), key=lambda x: x[1], reverse=True),
            }
        
        if code_language == 'python':
            metadata.update({
            f"Number of Modules in the {row_alias}:": len(module_counts),
            f"{row_alias} per Module:": sorted(module_counts.items(), key=lambda x: x[1], reverse=True),
            f"Size of code snippets per module (in KB):": sorted(module_sizes.items(), key=lambda x: x[1], reverse=True),
            })

        end_time = time.time()  # Record the end time
        self.dataset_processing_time = end_time - start_time
        print(f"Function '{functools.__name__}' took '{self.dataset_processing_time}' seconds to complete")

        return [table], metadata

    def detect_prog_lang(self, df, row, col):
        detected_language = ''
        # Check if 'code_language_col' is in DataFrame and its value matches 'python'
        if 'code_language_col' not in df.columns:
        # Fallback to use the detect_language function on the specific column
            detected_language = self.detect_language(row[col])  # replace 'col_name' with your target column for language detection
        elif 'code_language_col' in df.columns and row['code_language_col'].strip().lower() == 'python':
            detected_language = 'python'
        elif 'code_language_col' in df.columns and row['code_language_col'].strip().lower() == 'java':
            detected_language = 'java'
        return detected_language

    def package_data(self, row_alias, package_counts):
        package_results = {}
        package_results["stats"]= {
            f"Number of packages in the {row_alias}:": len(package_counts),
            f"{row_alias} per package:": sorted(package_counts.items(), key=lambda x: x[1], reverse=True),
            }
        self.package_data = package_results
        return package_results

    def python_module_data(self, row_alias, module_counts):
        module_results = {}
        module_results["stats"]= {
            f"Number of Modules in the {row_alias}:": len(module_counts),
            f"{row_alias} per Module:": sorted(module_counts.items(), key=lambda x: x[1], reverse=True),
            }
        self.module_data = module_results
        return module_results

    def package_size(self, row_alias, package_size):
        package_size_results = {}
        package_size_results["stats"]= {
            f"Size of the code snippets per package (in KB):": sorted(package_size.items(), key=lambda x: x[1], reverse=True),
            }
        self.package_size_data = package_size_results
        return package_size_results

    def python_module_size(self, row_alias, module_size_dict):
        module_size = {}
        module_size["stats"]= {
            f"Size of code snippets per module (in KB):": sorted(module_size_dict.items(), key=lambda x: x[1], reverse=True),
            }
        self.module_size_data = module_size
        return module_size

    def detect_language(self, code):
        try:
            lexer = lexers.guess_lexer(code)
            return lexer.aliases[0]
        except ClassNotFound:
            return None

    def list_static_packages(self, code_language, static_package_set):
        if code_language == "python":
            static_package_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "package_utils", "python"))
        if code_language == "java":
            static_package_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "package_utils", "java"))

        try:
            # List all files in the directory
            package_files = [f for f in os.listdir(static_package_dir) if f.endswith('.txt')]
            # Read each file and add its contents to the set
            for file_name in package_files:
                file_path = os.path.join(static_package_dir, file_name)
                with open(file_path, 'r') as file:
                    static_package_set.update(line.strip() for line in file if line.strip())
            return static_package_set
        except FileNotFoundError as e:
            print(f"Error: {e}. Please make sure the directory and files exist.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

