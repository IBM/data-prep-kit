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

from UAST import UAST
import json
from tree_sitter import Tree
import os
"""
Initialize the parser with a path for rules and grammar.
"""
class UASTParser():
    def __init__(self):
        self.language : str = None
        self.uast : UAST = None
        self.rules : dict = None
        self.cached_rules = dict()

        # Compute the absolute path to the tree-sitter-bindings directory
        grammar_dir = os.path.dirname(os.path.abspath(__file__))
        self.grammar_path = os.path.join(grammar_dir, '..', '..', 'python', 'src', 'grammar', 'UAST_Grammar.json')

        if not os.path.exists(self.grammar_path):
            print("Current working directory:", os.getcwd())
            raise FileNotFoundError(f"UAST Grammar file not found at {self.grammar_path}. Please ensure it exists.")

        with open(self.grammar_path, "r") as grammar_file:
            self.grammar = json.load(grammar_file)

        # Compute the absolute path to the ruleset directory based on the script's location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.rule_directory = os.path.join(script_dir, 'ruleset/')
        
        if not os.path.isdir(self.rule_directory):
            print("Script directory:", script_dir)
            raise FileNotFoundError(f"Ruleset directory not found at {self.rule_directory}. Please ensure it exists.")

        '''
        # Rule directory and file
        self.rule_directory = "../../python/src/ruleset/"
        if not os.path.isdir(self.rule_directory):
            print("Current working directory:", os.getcwd())
            raise FileNotFoundError(f"Ruleset directory not found at {self.rule_directory}. Please ensure it exists.")
        '''
        self.rule_file_name: str = "UAST_rules_"

        self.AST : Tree = None
        # self.offset : int = None
        # self.prev_line : int = -1
        self.extracted : str = None
        self.function_info = dict()
        self.class_info = dict()
        self.user_defined_entity = {"uast_function": "self.function_info[snippet] = id",
                         "uast_class": "self.class_info[snippet] = id"}


    def set_rule_dir_path(self, path: str):
        self.rule_directory = path

    def set_grammar_path(self, path : str):
        self.grammar_path = path
        self.grammar = json.load(open(self.grammar_path, "r"))

    # set language for the parser
    def set_language(self, language : str):
        self.language = language

        if (language not in self.cached_rules):
            rules_cache = json.load(open(self.rule_directory + self.rule_file_name + self.language + '.json', "r"))
            self.cached_rules[language] = rules_cache
        
        self.rules = self.cached_rules[language]

    # initialise a DFS traversal on the AST and an empty UAST.
    def parse(self, AST, code_snippet) :
        if(self.language == None) :
            print("Language not loaded")
            return
        self.AST = AST
        self.uast = UAST()
        self.uast.root.metadata["language"] = self.language
        self.uast.root.metadata["loc_snippet"] = self.count_loc(code_snippet, self.language)
        self._dfs(AST_node = self.AST.root_node, parent = self.uast.root)
        '''
        # commenting this block temporarily
        # Call the new modularized function to calculate the code-to-comment ratio
        code_to_comment_ratio = self.calculate_code_to_comment_ratio(self.uast.root)
        # Add the code_to_comment_ratio to the root node's metadata
        self.uast.root.metadata["code_to_comment_ratio"] = code_to_comment_ratio
        '''
        return self.uast

    def calculate_code_to_comment_ratio(self, root_node):
        # Get the loc_snippet from the root node's metadata
        loc_snippet = root_node.metadata.get("loc_snippet", 0)

        # Sum all loc_original_code for uast_comment nodes
        total_comment_loc = 0

        # Recursive function to sum comment LOC
        def sum_comment_loc(node):
            nonlocal total_comment_loc

            # Check if the node is a comment node
            if node.node_type == "uast_comment":
                total_comment_loc += node.metadata.get("loc_original_code", 0)

            # Traverse the children, ensuring we get the actual node objects
            for child_id in node.children:
                child_node = self.uast.get_node(child_id)  # Fetch the actual child node using self.uast
                sum_comment_loc(child_node)  # Recursively sum for the child node

        # Start summing loc_original_code from the root node
        sum_comment_loc(root_node)

        # Calculate the code-to-comment ratio (handling division by zero)
        if total_comment_loc > 0:
            return loc_snippet / total_comment_loc
        else:
            return None  # Handle no comments
        
    def count_lo_comments(self, code_snippet):
        lines = code_snippet.split('\n')
        loc_count = 0
        for line in lines:
            stripped_line = line.strip()
            # Count all lines except blank ones
            if stripped_line:
                loc_count += 1
        return loc_count

    def count_loc(self, code_snippet, language):
        # Define the comment markers for each language
        language_comment_markers = {
            "c": ('//', '/*', '*/'),
            "java": ('//', '/*', '*/'),
            "C#": ('//', '/*', '*/'),
            "c_sharp": ('//', '/*', '*/'),
            "cpp": ('//', '/*', '*/'),
            "objc": ('//', '/*', '*/'),
            "rust": ('//', '/*', '*/'),
            "go": ('//', '/*', '*/'),
            "kotlin": ('//', '/*', '*/'),
            "VHDL": ('--', None, None),
            "py": ('#', '"""', '"""'),
            "js": ('//', '/*', '*/'),
            "dart": ('//', '/*', '*/'),
            "QML": ('//', None, None),
            "typescript": ('//', '/*', '*/'),
            "perl": ('#', None, None),
            "haskell": ('--', '{-', '-}'),
            "elm": ('--', '{-', '-}'),
            "agda": ('--', '{-', '-}'),
            "d": ('//', '/*', '*/'),
            "nim": ('#', '##', None),
            "ocaml": ('(*', '(*', '*)'),
            "scala": ('//', '/*', '*/')
        }
        
        single_line_comment, multi_line_comment_start, multi_line_comment_end = language_comment_markers.get(language, (None, None, None))
        
        if not single_line_comment:
            raise ValueError(f"Unsupported language: {language}")
        
        lines = code_snippet.split('\n')
        loc_count = 0
        inside_multiline_comment = False

        for line in lines:
            stripped_line = line.strip()
            
            # Skip empty lines
            if not stripped_line:
                continue
            
            # Handle multi-line comments
            if multi_line_comment_start and multi_line_comment_end:
                if inside_multiline_comment:
                    # Check if the line contains the end of a multi-line comment
                    if multi_line_comment_end in stripped_line:
                        inside_multiline_comment = False
                    continue
                elif multi_line_comment_start in stripped_line:
                    # If the line starts a multi-line comment
                    inside_multiline_comment = True
                    continue
            
            # Skip single-line comments
            if stripped_line.startswith(single_line_comment):
                continue
            
            # If the line is neither a comment nor blank, count it as LOC
            loc_count += 1

        return loc_count

    def _add_user_defined(self, node):
        id = node.id
        type = node.node_type
        
        if node.code_snippet is not None:
            snippet = node.code_snippet.replace(type, '').strip()
            # Add further processing with the snippet
        else:
            # Handle the case where code_snippet is None
            snippet = ""
            # You can log a warning or take other appropriate action
            print(f"Warning: node.code_snippet is None for node type: {type}")

        if (type in self.user_defined_entity):
            exec(self.user_defined_entity[type])
            node.metadata["user_defined"] = True

        del id
        del type
        del snippet
        return

    # Traversing through the AST to create nodes recursively.
    def _dfs(self, AST_node, parent) :
        if (AST_node.type in self.rules) :
            ast_snippet = AST_node.text.decode("utf8")
            node_type = self.rules[AST_node.type]["uast_node_type"]
            exec_string = self.rules[AST_node.type]["extractor"]
            uast_snippet = self._extract(ast_snippet = ast_snippet, node_type = node_type, exec_string = exec_string)

            if node_type == "uast_comment":
                loc_original_code = self.count_lo_comments(ast_snippet)
            else:
                loc_original_code = self.count_loc(ast_snippet, self.language)

            node = self.uast.create_node(
                node_type = node_type,
                code_snippet = uast_snippet,
                # choose to enable or disbale the storage of original code by removing the following line.            
                metadata = {
                    "original_code" : ast_snippet,
                    "loc_original_code": loc_original_code
                },
            )
            self._add_user_defined(node)
            self.uast.add_edge(node1 = parent, node2 = node, directed_relation = "parent_node")
            parent = node

        for child in AST_node.children:
            self._dfs(AST_node= child, parent = parent)

    def _extract(self, ast_snippet, node_type, exec_string):
        code_snippet = ast_snippet
        try:
            exec(exec_string)
        except Exception as e:
            print(e)
        try:
            return self.grammar[node_type]["keyword"] + " " + self.extracted        
        except Exception as e:
            print(e)