from typing import Any

import argparse

from data_processing.utils.cli import CLIArgumentProvider
from data_processing.data_access.data_access import DataAccess
from data_processing.table_transform import AbstractTableTransform


class DefaultTableTransformRuntime(CLIArgumentProvider):
    """
    Transform runtime used by processor to create Transform specific environment
    """

    def __init__(self, transform_class:type[AbstractTableTransform]):
        """
        Create transform runtime
        :param params: parameters
        """
        self.transform_class = transform_class

    def set_data_access(self, data_access: DataAccess) -> None:
        """
        Set environment for transform execution
        :param data_access - data access class
        :return: dictionary of transform init params
        """
        pass

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        return stats

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        return None

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        return True


class AbstractTableTransformRuntimeFactory(CLIArgumentProvider):
    """
    Provides support the configuration of a transformer.
    """

    def __init__(
        self, runtime_class: type[DefaultTableTransformRuntime], transformer_class: type[AbstractTableTransform]
    ):
        """
        Initialization
        :param runtime_class: implementation of the Transform runtime
        :param transformer_class: implementation of the Transform
        :return:
        """
        self.runtime_class = runtime_class
        self.transformer_class = transformer_class

    def create_transform_runtime(self) -> DefaultTableTransformRuntime:
        """
        Create transform runtime
        :return: transform runtime object
        """
        return self.runtime_class()

    def get_transform_class(self) -> type[AbstractTableTransform]:
        """
        Create Mutator runtime
        :return: mutator class
        """
        return self.transformer_class

    def get_input_params_metadata(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return:
        """
        pass
