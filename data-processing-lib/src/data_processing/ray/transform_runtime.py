import argparse
from typing import Any

from data_processing.data_access import DataAccess
from data_processing.transform import AbstractTableTransform
from data_processing.utils import CLIArgumentProvider


class DefaultTableTransformRuntime:
    """
    This class is created in the ray orchestrator and provides orchestrator-side support for the transform running
    in the ray environment.  Creation/initialization is provided a dictionary of configuration value parsed from
    the command line as defined by the associate *TableTransformConfiguration's add_input_params() method.
    It provides the following key function:
        1) Using the set_environment() method, it can add ray-specific components (e.g. shared ray data object) to the
            initialization parameters provided to the Transform when it is created in the ray actor.
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters generally parsed from the command line as defined by the arguments added in
        the associated *TableTransformConfiguration's add_input_params() method.
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
    Because this class is passed between ray nodes, this class and extensions must be python pickleable.
    This implementation holds/defines the following:
        1) defines a set of optional CLI arguments that are provided by the ray orchestration to
            both the Transform initializer and the Runtime initializer.
        2) holds the class of the concrete AbstractTransform class or extension having an initializer that takes a
            dictionary of configuration key/value pairs. The dictionary provided for initialization is defined by
            the CLI arguments defined in this class via add_input_params().  AbstractTransform-extending class
            will be created in the ray worker to perform the table transformations.
        3) holds the class of the DefaultTableTransformRuntime or extension that supports operation of the transform
            on the ray orchestrator side.  It is created with an initializer that takes the dictionary
            of CLI arguments, optionally defined in this class, similar to the transform.
    To define configuration data provided to both of the transform and runtime classes during initialization,
    sub-classes should provide an implementation of add_input_params().  Argument names defined here must
    then also be provided to this class's initializer in the cli_argnames list.
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
        Get the concrete class extending AbstractTableTransform which is to be run in the ray actor.
        :return: transform class
        """
        return self.transform_class
