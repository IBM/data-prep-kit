from typing import Any

from data_processing.utils.cli import CLIArgumentProvider
from data_processing.data_access.data_access import DataAccess
from data_processing.table_transform import AbstractTableTransform


class DefaultTableTransformRuntime:
    """
    Transform runtime used by processor to create Transform specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create transform runtime
        :param params: parameters
        """
        self.params = params

    def set_environment(self, data_access: DataAccess) -> dict[str, Any]:
        """
        Set environment for transform execution
        :param data_access - data access class
        :return: dictionary of transform init params
        """
        return self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        return stats


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
        self.params = {}

    def create_transformer_runtime(self) -> DefaultTableTransformRuntime:
        """
        Create transform runtime
        :return: transform runtime object
        """
        return self.runtime_class(self.params)

    def get_transformer_class(self) -> type[AbstractTableTransform]:
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
