from data_processing.runtime.pure_python import PythonLauncherConfiguration
from data_processing.runtime.ray import DefaultTableTransformRuntimeRay
from data_processing.transform import TransformConfiguration


class RayLauncherConfiguration(PythonLauncherConfiguration):
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
        transform_configuration: type[TransformConfiguration],
        runtime_class: type[DefaultTableTransformRuntimeRay] = DefaultTableTransformRuntimeRay,
    ):
        """
        Initialization
        :param transform_class: implementation of the transform
        :param runtime_class: implementation of the transform runtime
        :param base: base transform configuration class
        :param name: transform name
        :param remove_from_metadata: list of parameters to remove from metadata
        :return:
        """
        super().__init__(
            transform_configuration=transform_configuration,
        )
        self.runtime_class = runtime_class

    def create_transform_runtime(self) -> DefaultTableTransformRuntimeRay:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.params)
