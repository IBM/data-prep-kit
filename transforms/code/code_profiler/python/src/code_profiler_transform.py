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

import os
import subprocess
from argparse import ArgumentParser, Namespace
from typing import Any
from data_processing.utils import get_logger
import uuid
import shutil
import atexit

import pyarrow as pa
from data_processing.transform import AbstractTableTransform
from tree_sitter import Language, Parser as TSParser
from tree_sitter_languages import get_language


from UAST_parser import UASTParser
import json
from data_processing.transform import AbstractBinaryTransform, TransformConfiguration

from data_processing.utils import (
    CLIArgumentProvider,
    get_logger,
)

short_name = "CodeProfiler"
cli_prefix = f"{short_name}_"
language_key = "language"
contents_key = "contents"
language_cli_param = f"{cli_prefix}{language_key}"
contents_cli_param = f"{cli_prefix}{contents_key}"

class CodeProfilerTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """

        super().__init__(config)
        
        self.contents = self.config.get("contents")
        self.language = self.config.get("language")

        def ensure_tree_sitter_bindings():
            # Get the directory where the script is located
            script_dir = os.path.dirname(os.path.abspath(__file__))
            # Generate a unique directory for the bindings based on a UUID
            bindings_dir = os.path.join(script_dir, f"tree-sitter-bindings-{uuid.uuid4()}")
            # Clone the bindings only if the unique directory does not exist
            if not os.path.exists(bindings_dir):
                print(f"Cloning tree-sitter bindings into {bindings_dir}...")
                result = subprocess.run(["git", "clone", "https://github.com/pankajskku/tree-sitter-bindings.git", bindings_dir])
                if result.returncode != 0:
                    raise RuntimeError(f"Failed to clone tree-sitter bindings into {bindings_dir}")
                return bindings_dir         

        # Call this function before the main code execution
        self.bindings_dir = ensure_tree_sitter_bindings()

        # Use the correct architecture for runtime
        RUNTIME_HOST_ARCH = os.environ.get('RUNTIME_HOST_ARCH', 'x86_64')
        bindings_path = self.bindings_dir + '/' + RUNTIME_HOST_ARCH # for MAC: mach-arm64
        print(f"Bindings bindings_dir: {self.bindings_dir}")
        print(f"Bindings path: {bindings_path}")

        # Check if the bindings path exists
        if not os.path.exists(bindings_path):
            raise FileNotFoundError(f"Bindings path does not exist: {bindings_path}")

        C_LANGUAGE = get_language('c')
        CPP_LANGUAGE = get_language("cpp")
        CSHARP_LANGUAGE = Language(os.path.join(bindings_path, 'c_sharp-bindings.so'), 'c_sharp')
        D_LANGUAGE = Language(os.path.join(bindings_path, 'd-bindings.so'), 'd')
        DART_LANGUAGE = Language(os.path.join(bindings_path, 'dart-bindings.so'), 'dart')
        GOLANG_LANGUAGE = Language(os.path.join(bindings_path, 'go-bindings.so'), 'go')
        JAVA_LANGUAGE = get_language("java")
        JAVASCRIPT_LANGUAGE = Language(os.path.join(bindings_path, 'js-bindings.so'), 'javascript')
        NIM_LANGUAGE = Language(os.path.join(bindings_path, 'nim-bindings.so'), 'nim')
        #OBJECTIVE_C_LANGUAGE = Language(os.path.join(bindings_path, 'objc-bindings.so'), 'objc')
        OCAML_LANGUAGE = get_language("ocaml")
        PERL_LANGUAGE = get_language("perl")
        PY_LANGUAGE = get_language("python")
        RUST_LANGUAGE = get_language("rust")
        SCALA_LANGUAGE = Language(os.path.join(bindings_path, 'scala-bindings.so'), 'scala')
        TYPESCRIPT_LANGUAGE = get_language("typescript")

        # Language map for supported languages
        self.language_map = {
            "C": C_LANGUAGE,
            "C#": CSHARP_LANGUAGE,
            "Cpp": CPP_LANGUAGE,
            "D": D_LANGUAGE,
            "Dart": DART_LANGUAGE,
            "Go": GOLANG_LANGUAGE,
            "Java": JAVA_LANGUAGE,
            "JavaScript": JAVASCRIPT_LANGUAGE,
            "Nim": NIM_LANGUAGE,
            "Ocaml": OCAML_LANGUAGE,
            #"Objective-C": OBJECTIVE_C_LANGUAGE,
            "Perl": PERL_LANGUAGE,
            "Python": PY_LANGUAGE, 
            "Rust": RUST_LANGUAGE,
            "Scala": SCALA_LANGUAGE,
            "TypeScript": TYPESCRIPT_LANGUAGE
        }
        self.uast_language_map = {
            "C": 'c',
            "C#": 'c_sharp',
            "C++": 'cpp',
            "Cpp": 'cpp',
            "D": 'd',
            "Dart": 'dart',
            "Go": 'go',
            "Java": 'java',
            "JavaScript": 'js',
            "Nim": 'nim',
            "Ocaml": 'ocaml',
            #"Objective-C": 'objc',
            "Perl": 'perl',
            "Python": 'py',
            "Rust": 'rust',
            "Scala": 'scala',
            "TypeScript": 'typescript'
        }
        self.logger = get_logger(__name__)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Extracts the syntactic constructs
        """
        print("tranforming the the input dataframe")

        ts_parser = TSParser()
        uast_parser = UASTParser()

        def get_uast_json(code, lang):
            if lang in self.language_map:
                ts_parser.set_language(self.language_map[lang])
                uast_parser.set_language(self.uast_language_map[lang])
                ast = ts_parser.parse(bytes(code, encoding= "utf8"))
                uast = uast_parser.parse(ast, code)
                return uast.get_json()
            return None

        def get_uast_parquet():
            # df = pd.read_parquet(f'{db_path}/{filename}', 'pyarrow')
            # df = df.reindex(columns=all_columns)
        
            # Extract language and content arrays from the table using PyArrow
            lang_array = table.column(self.language)
            content_array = table.column(self.contents)
            # Ensure both arrays have the same length
            assert len(lang_array) == len(content_array)
            # Generate UASTs using a list comprehension
            uasts = [json.dumps(get_uast_json(content_array[i].as_py(), lang_array[i].as_py())) for i in range(len(content_array))]     
            # Add the UAST array as a new column in the PyArrow table
            uast_column = pa.array(uasts)
            table_with_uast = table.append_column('UAST', uast_column)
            return table_with_uast

        table_with_uast = get_uast_parquet()
        # report statistics
        stats = {"source_documents": table.num_columns, "result_documents": table_with_uast.num_columns}

        # Register cleanup for when the process exits
        atexit.register(shutil.rmtree, self.bindings_dir)

        return [table_with_uast], stats
    
class CodeProfilerTransformConfiguration(TransformConfiguration):
    def __init__(self, transform_class: type[AbstractBinaryTransform] = CodeProfilerTransform):
        super().__init__(
            name=short_name,
            transform_class=transform_class,
            )
    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{language_cli_param}",
            type=str,
            default="Language",
            help="Column name that denotes the programming language",
        )
        parser.add_argument(
            f"--{contents_cli_param}",
            type=str,
            default="Contents",
            help="Column name that contains code snippets",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = captured
        return True
