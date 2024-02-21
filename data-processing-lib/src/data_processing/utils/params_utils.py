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
    def dict_to_req(d: dict[str, Any]) -> list[str]:
        """
        Convert dictionary to a list of string parameters
        :param d: dictionary
        :return: an array of parameters
        """
        res = [""]
        for key, value in d.items():
            res.append(f"--{key}={value}")
        return res
