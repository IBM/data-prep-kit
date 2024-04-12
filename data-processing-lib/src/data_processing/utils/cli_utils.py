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

import argparse
from typing import Any


KB = 1024
MB = 1024 * KB
GB = 1024 * MB


def str2bool(value: str) -> bool:
    """
    Convert string to boolean. Helper for getting boolean parameters
    :param value - input string
    """
    if value.strip().lower() in ("yes", "true", "t", "y", "1"):
        return True
    return False


class CLIArgumentProvider:
    """
    Interface for the implementation of the classes that populate parser
    with the required information and apply/validate user provided information
    """

    @staticmethod
    def capture_parameters(args: argparse.Namespace, prefix: str, keep_prefix: bool = True):
        """
        Converts a namespace of values into a dictionary of keys and values where the keys
        match the given prefix.
        :param args: namespace instance to read keys/values from
        :param prefix: optional prefix to restrict the set of namespace keys considered for inclusion in the returned dictionary
        :param keep_prefix:  controls whether or not the prefix is stripped from the keys in the resulting dictionary.
        :return:  a dictionary of keys matching the prefix and their values.  The keys in the dictionary may or may not include the prefix.
        """
        captured = {}
        args_dict = vars(args)
        for key, value in args_dict.items():
            if prefix is None or key.startswith(prefix):
                if prefix is not None and not keep_prefix:
                    key = key.replace(prefix, "")
                captured[key] = value
        return captured

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add arguments to the given parser.
        :param parser: parser
        :return:
        """
        pass

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments including at least, but perhaps more,
        arguments as defined by add_input_arguments().
        :return: True, if validate pass or False otherwise
        """
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        return self.params
