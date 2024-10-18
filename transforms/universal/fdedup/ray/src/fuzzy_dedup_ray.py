import argparse
import os
import sys

from cluster_analysis_transform_ray import ClusterAnalysisRayTransformConfiguration
from data_cleaning_transform_ray import DataCleaningRayTransformConfiguration
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from data_processing_ray.runtime.ray import RayTransformLauncher
from fuzzy_dedup_python import ServiceOrchestrator, parse_args
from get_duplicate_list_transform_python import (
    GetDuplicateListPythonTransformConfiguration,
)
from signature_calc_transform_ray import SignatureCalculationRayTransformConfiguration


s3_creds = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "url": os.getenv("AWS_ENDPOINT_URL"),
}


ray_worker_options = {"num_cpus": 0.8}
ray_params = {
    # where to run
    "run_locally": True,
    # orchestrator
    "runtime_worker_options": ParamsUtils.convert_to_ast(ray_worker_options),
    "runtime_num_workers": 3,
}

ray_params_argv = ParamsUtils.dict_to_req(ray_params)


class RayServiceOrchestrator(ServiceOrchestrator):
    def __init__(self, global_params: argparse.Namespace = None):
        super().__init__(global_params=global_params)

    def execute_service(self, service_short_name: str, params: list) -> int:
        sys.argv = params if service_short_name == "fdlist" else ray_params_argv + params[1:]
        if service_short_name == "minhash":
            launcher = RayTransformLauncher(runtime_config=SignatureCalculationRayTransformConfiguration())
        elif service_short_name == "cluster":
            launcher = RayTransformLauncher(runtime_config=ClusterAnalysisRayTransformConfiguration())
        elif service_short_name == "fdlist":
            launcher = PythonTransformLauncher(runtime_config=GetDuplicateListPythonTransformConfiguration())
        elif service_short_name == "fdclean":
            launcher = RayTransformLauncher(runtime_config=DataCleaningRayTransformConfiguration())
        status = launcher.launch()
        return status


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    # Initialize the orchestrator
    orchestrator = RayServiceOrchestrator(global_params=args)
    # Launch ray fuzzy dedup execution
    orchestrator.orchestrate()
