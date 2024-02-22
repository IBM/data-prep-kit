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

    def __init__(self, keys: list[str] = None):
        """
        Provides support for the implementation of get_input_params() when keys is provided.
        :param keys:  a list of argument names as used in add_input_params().  If provided,
        and this implementation of apply_input_params() is called, then get_input_params()
        will return a dictionary containing the given keys and values parsed by argparse.
        If keys is provided and overriding apply_input_params() you will need to call this
        implementation to get the key values returned by get_input_params().
        """
        self.keys = keys
        self.params = None

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
        #
        # If the user has provided a set of keys, then extract them from the
        # given name space so that we can return them in get_input_params()
        if self.keys is not None:
            self.params = {}
            args_as_dict = vars(args)
            for key in self.keys:
                value = args_as_dict.get(key, None)
                if value is not None:
                    self.params[key] = value
        else:
            self.params = None
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        if self.params is None:
            raise NotImplemented("No keys were provided at initialization which are used to extract our parameters")
        return self.params
