from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.runtime import AbstractTransformConfiguration


class AbstractTransformLauncher:
    def __init__(
        self,
        transform_config: AbstractTransformConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param transform_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        self.transform_config = transform_config
        self.name = self.transform_config.get_name()
        self.data_access_factory = data_access_factory

    def launch(self):
        raise ValueError("must be implemented by subclass")

    def get_transform_name(self) -> str:
        return self.name
