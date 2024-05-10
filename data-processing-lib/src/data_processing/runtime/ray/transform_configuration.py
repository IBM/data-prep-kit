from data_processing.runtime.ray import DefaultTableTransformRuntimeRay
from data_processing.transform.transform_configuration import (
    TransformConfiguration,
    TransformConfigurationProxy,
)


class RayTransformConfiguration(TransformConfigurationProxy):
    def __init__(
        self,
        transform_config: TransformConfiguration,
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
            proxied_transform_config=transform_config,
        )
        self.runtime_class = runtime_class

    def create_transform_runtime(self) -> DefaultTableTransformRuntimeRay:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.params)
