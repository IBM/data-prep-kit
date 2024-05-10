from data_processing.launch import TransformConfiguration
from data_processing.launch.ray import DefaultTableTransformRuntimeRay
from data_processing.transform import AbstractTableTransform


class RayTransformConfiguration(TransformConfiguration):

    def __init__(self, name:str, transform_class:type[AbstractTableTransform],
                 runtime_class:type[DefaultTableTransformRuntimeRay] = DefaultTableTransformRuntimeRay,
                 remove_from_metadata:list[str]= []):
        super().__init__(name,transform_class, remove_from_metadata)
        self.runtime_class = runtime_class
