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

from typing import Any


class ParamsUtils:
    """
    Class implementing support methods for parameters manipulation
    """

    @staticmethod
    def convert_to_ast(d: dict[str, Any]) -> str:
        """
        Converts dictionary to AST string representing the dictionary
        :param d: dictionary
        :return: an AST string
        """
        ast_string = "{"
        first = True
        for key, value in d.items():
            if first:
                first = False
            else:
                ast_string += ", "
            if isinstance(value, str):
                ast_string += f"'{key}': '{value}'"
            else:
                ast_string += f"'{key}': {value}"
        ast_string += "}"
        return ast_string

    @staticmethod
    def dict_to_req(d: dict[str, Any], executor: str = "") -> list[str]:
        """
        Convert dictionary to a list of string parameters
        :param executor - executor name
        :param d: dictionary
        :return: an array of parameters
        """
        if executor != "":
            # local testing
            res = [executor]
        else:
            # remote invoke
            res = [f"python {executor}"]
        for key, value in d.items():
            res.append(f"--{key}={value}")
        return res

    @staticmethod
    def __dict_to_str(dict_val: dict[str, str], initial_indent: str, indent_per_level: str, as_value: bool) -> str:
        all_text = ""
        if as_value:
            all_text = all_text + "{ "
        first = True
        last_line = ""
        for key, value in dict_val.items():
            if isinstance(value, dict):
                text = ParamsUtils.__dict_to_str(value, initial_indent + indent_per_level, indent_per_level, as_value)
            else:
                if as_value:
                    key = "'" + key + "'"
                    if isinstance(value, str):
                        value = "'" + value + "'"
                text = initial_indent + key + ": " + str(value)
            if first:
                new_text = ""
            elif as_value:
                new_text = ", "
            else:
                new_text = "\n"
            if as_value and len(last_line) + len(text) > 60:
                new_text = new_text + "\n"
                last_line = ""
            new_text = new_text + text
            all_text = all_text + new_text
            last_line = last_line + new_text
            first = False
        all_text = all_text.strip()
        if as_value:
            all_text = all_text + " }"
        return all_text

    @staticmethod
    def get_ast_help_and_example_text(help_dict: dict[str, str], examples: list[dict[str, Any]]):
        initial_indent = ""
        indent_per_level = "   "
        help_txt = ParamsUtils.__dict_to_str(help_dict, initial_indent, indent_per_level, False)
        if examples is not None:
            example_txt = "\n" + initial_indent
            if len(examples) == 1:
                example_txt += "Example: "
            else:
                example_txt += "Example(s):"
            for example_dict in examples:
                etxt = ParamsUtils.__dict_to_str(example_dict, initial_indent, indent_per_level, True)
                if len(examples) == 1:
                    example_txt = example_txt + etxt
                else:
                    example_txt = example_txt + "\n" + initial_indent + "    " + etxt
        else:
            example_txt = ""
        msg = help_txt + example_txt
        return msg

    @staticmethod
    def get_ast_help_text(help_example_dict: dict[str, list[str, Any]]):
        """
        Create some help text for an AST-formatted parameter value.
        :param help_example_dict:  This dictionary of lists, where they keys
        correspond to the parameter names and the list is a pair of values.
        The 1st value in the list is an example value for the option, the 2nd in is the help text.
        If you need to provide more than 1 example, use get_ast_help_and_example_text() which
        allows a list of examples.
        Example:
            help_example_dict = {
                'access_key': ["access", 'access key help text'],
                'secret_key': ["secret", 'secret key help text'],
                'url': ['https://s3.us-east.cloud-object-storage.appdomain.cloud', "s3 url"]
            }
            parser.add_argument(
                "--s3_cred",
                type=ast.literal_eval,
                default=None,
                help="ast string of options for s3 credentials\n" +
                     ParamsUtils.get_ast_help_text(help_example_dict)
            )
        :return:  a string to be included in help text, usually concantentated with the general
        parameter help text.
        """

        help_dict = {}
        example_dict = {}
        for key, value in help_example_dict.items():
            if not isinstance(value, list):
                raise ValueError("key value for key " + key + " is not a list")
            if len(value) != 2:
                raise ValueError("List for key " + key + " is not a list of length 2")
            example_value = value[0]
            help_text = value[1]
            help_dict[key] = help_text
            example_dict[key] = example_value
        return ParamsUtils.get_ast_help_and_example_text(help_dict, [example_dict])
