import argparse

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

