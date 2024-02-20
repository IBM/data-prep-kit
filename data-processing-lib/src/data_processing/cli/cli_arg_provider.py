import argparse
import ast


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
    with the required information and validate user provided information
    """

    def define_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameter to parser
        :param parser: parser
        :return:
        """
        pass

    def validate_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        return True

