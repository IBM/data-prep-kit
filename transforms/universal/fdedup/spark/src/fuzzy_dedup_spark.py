import argparse
import os
import sys

from cluster_analysis_transform_spark import ClusterAnalysisSparkTransformConfiguration
from data_cleaning_transform_spark import DataCleaningSparkTransformConfiguration
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing_spark.runtime.spark import SparkTransformLauncher
from fuzzy_dedup_python import ServiceOrchestrator, parse_args
from get_duplicate_list_transform_python import (
    GetDuplicateListPythonTransformConfiguration,
)
from signature_calc_transform_spark import (
    SignatureCalculationSparkTransformConfiguration,
)


s3_creds = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "url": os.getenv("AWS_ENDPOINT_URL"),
}


class SparkServiceOrchestrator(ServiceOrchestrator):
    def __init__(self, global_params: argparse.Namespace = None):
        super().__init__(global_params=global_params)

    def execute_service(self, service_short_name: str, params: list) -> int:
        sys.argv = params
        if service_short_name == "minhash":
            launcher = SparkTransformLauncher(runtime_config=SignatureCalculationSparkTransformConfiguration())
        elif service_short_name == "cluster":
            launcher = SparkTransformLauncher(runtime_config=ClusterAnalysisSparkTransformConfiguration())
        elif service_short_name == "fdlist":
            launcher = PythonTransformLauncher(runtime_config=GetDuplicateListPythonTransformConfiguration())
        elif service_short_name == "fdclean":
            launcher = SparkTransformLauncher(runtime_config=DataCleaningSparkTransformConfiguration())
        status = launcher.launch()
        return status


if __name__ == "__main__":

    # Parse command line arguments
    args = parse_args()
    # Initialize the orchestrator
    orchestrator = SparkServiceOrchestrator(global_params=args)
    # Launch spark fuzzy dedup execution
    orchestrator.orchestrate()
