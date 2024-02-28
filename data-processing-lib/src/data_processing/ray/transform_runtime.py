from typing import Any

from data_processing.data_access import DataAccessFactory
from data_processing.transform import AbstractTableTransform
from data_processing.utils import CLIArgumentProvider
from ray.actor import ActorHandle
from typing_extensions import deprecated


class DefaultTableTransformRuntime:
    """
    Transformer runtime used by processor to to create Mutator specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TableTansformConfiguration.
        """
        self.params = params

    def get_transform_config(
        self, data_access_factory: DataAccessFactory, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc, that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :param files - list of files to process
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


class DefaultTableTransformConfiguration(CLIArgumentProvider):
    """
    Provides support the configuration of a transformer running in the ray environment.
    It holds the following:
        1) The type of the concrete AbstractTransform class, that is created by a the ray worker with a
            dictionary of parameters to perform that table transformations.
        2) The type of the of DefaultTableTransformRuntime that supports operation of the transform
            on the ray orchestrator side.  It is create with an initializer that takes the dictionary
            of CLI arguments, optionally defined in this class.
    Sub-classes may extend this class to override the following:
        1) add_input_params() to add CLI argument definitions used in creating both the AbstractTransform
            and the DefaultTableTransformRuntime.
    """

    def __init__(
        self,
        name: str,
        transform_class: type[AbstractTableTransform],
        runtime_class: type[DefaultTableTransformRuntime] = DefaultTableTransformRuntime,
    ):
        """
        Initialization
        :param transform_class: implementation of the Filter
        :param runtime_class: implementation of the Filter runtime
        :return:
        """
        self.name = name
        self.runtime_class = runtime_class
        self.transform_class = transform_class
        # These are expected to be updated later by the sub-class in apply_input_params().
        self.params = {}

    def create_transform_runtime(self) -> DefaultTableTransformRuntime:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.params)

    def get_transform_class(self) -> type[AbstractTableTransform]:
        """
        Get the class extending AbstractTableTransform which implements a specific transformation.
        The class will generally be instantiated with a dictionary of configuration produced by
        the associated TransformRuntime's get_transform_config() method.
        :return: class extending AbstractTableTransform
        """
        return self.transform_class

    def get_name(self):
        return self.name
