import argparse

from data_processing.runtime import TransformExecutionConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger


cli_prefix = "spark_"

local_config_path_cli = f"{cli_prefix}local_config_filepath"
local_config_path_default = "config/spark_profile_local.yml"

kube_config_path_cli = f"{cli_prefix}kube_config_filepath"
kube_config_path_default = "config/spark_profile_kube.yml"

logger = get_logger(__name__)


class SparkExecutionConfiguration(TransformExecutionConfiguration):
    def __init__(self, name: str, log: bool = False):
        super().__init__(name, log)
        self.local_config_filepath = None
        self.kube_config_filepath = None

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        super().add_input_params(parser)
        parser.add_argument(
            f"--{local_config_path_cli}",
            type=str,
            default=local_config_path_default,
            help="Path to spark configuration for run",
        )
        parser.add_argument(
            f"--{kube_config_path_cli}",
            type=str,
            default=kube_config_path_default,
            help="Path to Kubernetes-based configuration.",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        super().apply_input_params(args)
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, True)
        logger.info(f"spark execution config : {captured}")
        self.local_config_filepath = captured.get(local_config_path_cli)
        self.kube_config_filepath = captured.get(kube_config_path_cli)
        return True
