import argparse
from typing import Any

from data_processing.data_access import DataAccess
from data_processing.transform import AbstractTableTransform
from data_processing.utils import CLIArgumentProvider


class DefaultTableTransformRuntime:
    """
    Transformer runtime used by processor to to create Mutator specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters
        """
        self.params = params

    def set_environment(self, data_access: DataAccess) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access - data access class
        :return: dictionary of filter init params
        """
        return self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        return stats


class DefaultTableTransformConfiguration(CLIArgumentProvider):
    """
    Provides support the configuration of a transformer running in the ray environment.
    It holds the following:
        1) The type of the concrete AbstractTransform class having a zero-args initializer. This
            will be created in the ray worker to perform that table transformations.
        2) The type of the of DefaultTableTransformRuntime that supports operation of the transform
            on the ray orchestrator side.  It is create with an initializer that takes the dictionary
            of CLI arguments, optionally defined in this class.
    Sub-classes may extend this class to override the following:
        1) add_input_params() to add CLI argument definitions used in creating both the AbstractTransform
            and the DefaultTableTransformRuntime.
    """

    def __init__(
        self,
        cli_argnames: list[str],
        transform_class: type[AbstractTableTransform],
        runtime_class: type[DefaultTableTransformRuntime] = DefaultTableTransformRuntime,
    ):
        super().__init__(cli_argnames)
        """
        Initialization
        :param cli_argnames: list of strings naming the arguments defined in add_input_arguments() without the -- or -.
        :param transform_class: implementation of the Filter
        :param runtime_class: implementation of the Filter runtime
        :return:
        """
        self.runtime = runtime_class
        self.transform_class = transform_class
        self.params = {}

    def create_transform_runtime(self) -> DefaultTableTransformRuntime:
        """
        Create Filter runtime
        :return: fiter runtime object
        """
        return self.runtime(self.params)

    def get_transform_class(self) -> type[AbstractTableTransform]:
        """
        Create Mutator runtime
        :return: mutator class
        """
        return self.transform_class
