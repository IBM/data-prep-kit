import argparse
from argparse import ArgumentParser

from data_processing.data_access import (
    DataAccess,
    DataAccessFactory,
    DataAccessFactoryBase,
)
from data_processing.runtime import (
    AbstractTransformLauncher,
    TransformExecutionConfiguration,
)
from data_processing.utils import get_logger, str2bool
from data_processing_spark.runtime.spark.runtime_config import (
    SparkTransformRuntimeConfiguration,
)
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform


logger = get_logger(__name__)


class SparkTransformLauncher(AbstractTransformLauncher):
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        runtime_config: SparkTransformRuntimeConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        super().__init__(runtime_config, data_access_factory)
        self.runtime_config = runtime_config
        self.execution_config = TransformExecutionConfiguration(runtime_config.get_name())

    def launch(self):
        if not self._get_args():
            logger.warning("Arguments could not be applied.")
            return 1
        transform_params = dict(self.runtime_config.get_transform_params())
        transform_class = self.runtime_config.get_transform_class()
        transform = transform_class(transform_params)
        data_access = self.data_access_factory.create_data_access()
        self._start_spark()
        self._run_transform(data_access, transform)
        self._stop_spark()

    def _start_spark(self):
        pass

    def _stop_spark(self):
        pass

    def _run_transform(self, data_access: DataAccess, transform: AbstractSparkTransform):
        pass

    def _get_args(self) -> bool:
        parser = argparse.ArgumentParser(
            description=f"Driver for {self.name} processing on Spark",
            # RawText is used to allow better formatting of ast-based arguments
            # See uses of ParamsUtils.dict_to_str()
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self._add_input_params(parser)
        args = parser.parse_args()
        return self._apply_input_params(args)

    def _add_input_params(self, parser):

        parser.add_argument(
            "--run_locally", type=lambda x: bool(str2bool(x)), default=False, help="running ray local flag"
        )
        # add additional arguments
        self.runtime_config.add_input_params(parser=parser)
        self.data_access_factory.add_input_params(parser=parser)
        self.execution_config.add_input_params(parser=parser)

    def _apply_input_params(self, args: argparse.Namespace):
        self.run_locally = args.run_locally
        if self.run_locally:
            logger.info("Running locally")
        else:
            logger.info("connecting to existing cluster")
        return (
            self.runtime_config.apply_input_params(args=args)
            and self.data_access_factory.apply_input_params(args=args)
            and self.execution_config.apply_input_params(args=args)
        )
