import argparse
import os
import sys

import cluster_analysis_transform
import data_cleaning_transform
import get_duplicate_list_transform
import signature_calc_transform
from cluster_analysis_transform_python import (
    ClusterAnalysisPythonTransformConfiguration,
)
from data_cleaning_transform_python import DataCleaningPythonTransformConfiguration
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils, get_logger
from get_duplicate_list_transform_python import (
    GetDuplicateListPythonTransformConfiguration,
)
from signature_calc_transform_python import (
    SignatureCalculationPythonTransformConfiguration,
)


SERVICE_DICT = {
    "SignatureCalculation": "minhash",
    "ClusterAnalysis": "cluster",
    "GetDuplicateList": "fdlist",
    "DataCleaning": "fdclean",
}

s3_creds = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "url": os.getenv("AWS_ENDPOINT_URL"),
}

ARGS_MAP = {
    "minhash": [
        signature_calc_transform.contents_column_key,
        signature_calc_transform.document_id_column_key,
        signature_calc_transform.seed_key,
        signature_calc_transform.num_permutations_key,
        signature_calc_transform.num_bands_key,
        signature_calc_transform.num_minhashes_per_band_key,
        signature_calc_transform.jaccard_similarity_threshold_key,
        signature_calc_transform.word_shingle_size_key,
        signature_calc_transform.num_segments_key,
    ],
    "cluster": [
        cluster_analysis_transform.jaccard_similarity_threshold_key,
        cluster_analysis_transform.num_bands_key,
        cluster_analysis_transform.num_segments_key,
    ],
    "fdlist": [
        get_duplicate_list_transform.subfolder_key,
        get_duplicate_list_transform.consolidated_filename_key,
    ],
    "fdclean": [
        data_cleaning_transform.document_id_column_key,
        data_cleaning_transform.duplicate_list_location_key,
        data_cleaning_transform.operation_mode_key,
    ],
}


class ServiceOrchestrator:
    def __init__(self, global_params: argparse.Namespace = None):
        self.global_params = global_params
        self.logger = get_logger(__name__)

    def orchestrate(self):
        service_list = self.global_params.services.split(",")
        for service in service_list:
            self.logger.info(f"Starting {service} step")
            if service not in SERVICE_DICT:
                err_msg = f"Unknown service {service} specified. Must be one of {SERVICE_DICT.keys()}"
                self.logger.error(err_msg)
                raise ValueError(err_msg)
            service_short_name = SERVICE_DICT[service]
            service_params = self.get_arguments(args, service_short_name)
            self.logger.info(f"Got parameters for {service}")
            status = self.execute_service(service_short_name, service_params)
            if status == 0:
                self.logger.info(f"{service} completed successfully")
            else:
                self.logger.error(f"{service} failed with status {status}, aborting ...")
                break

    def get_arguments(self, in_args: argparse.Namespace, service_name: str) -> list:
        sys_argv = ["python"]
        in_args_dict = vars(in_args)
        all_module_arguments = ARGS_MAP.get(service_name, [])
        passed_args = {k: v for k, v in in_args_dict.items() if k in all_module_arguments and v is not None}
        for k, v in passed_args.items():
            sys_argv.append(f"--{service_name}_{k}")
            sys_argv.append(str(v))
        if service_name == "minhash":
            input_folder = in_args_dict["input_folder"]
            output_folder = in_args_dict["output_folder"]
        elif service_name == "cluster":
            input_folder = os.path.join(in_args_dict["output_folder"], "bands")
            output_folder = os.path.join(in_args_dict["output_folder"], "docs_to_remove")
        elif service_name == "fdlist":
            input_folder = in_args_dict["output_folder"]
            output_folder = in_args_dict["output_folder"]
        elif service_name == "fdclean":
            input_folder = in_args_dict["input_folder"]
            operation_mode = in_args_dict.get("operation_mode", "filter_duplicates")
            if operation_mode == "filter_duplicates":
                output_subfolder = "cleaned"
            elif operation_mode == "filter_non_duplicates":
                output_subfolder = "duplicates"
            else:  # operation_mode == "annotate"
                output_subfolder = "annotated"
            output_folder = os.path.join(in_args_dict["output_folder"], output_subfolder)
        else:
            self.logger.error(f"Unknown service name: {service_name}")
        data_io = {
            "input_folder": input_folder,
            "output_folder": output_folder,
        }
        if in_args.use_s3:
            sys_argv.append("--data_s3_cred")
            sys_argv.append(ParamsUtils.convert_to_ast(s3_creds))
            sys_argv.append("--data_s3_config")
        else:
            sys_argv.append("--data_local_config")
        sys_argv.append(ParamsUtils.convert_to_ast(data_io))
        return sys_argv

    def execute_service(self, service_short_name: str, params: list) -> int:
        sys.argv = params
        if service_short_name == "minhash":
            launcher = PythonTransformLauncher(runtime_config=SignatureCalculationPythonTransformConfiguration())
        elif service_short_name == "cluster":
            launcher = PythonTransformLauncher(runtime_config=ClusterAnalysisPythonTransformConfiguration())
        elif service_short_name == "fdlist":
            launcher = PythonTransformLauncher(runtime_config=GetDuplicateListPythonTransformConfiguration())
        elif service_short_name == "fdclean":
            launcher = PythonTransformLauncher(runtime_config=DataCleaningPythonTransformConfiguration())
        status = launcher.launch()
        return status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Service Orchestrator")

    # Define command line arguments
    parser.add_argument("--input_folder", type=str, required=True, help="Input folder path")
    parser.add_argument("--output_folder", type=str, required=True, help="Output folder path")

    parser.add_argument(
        "--operation_mode",
        choices=["filter_duplicates", "filter_non_duplicates", "annotate"],
        required=False,
        help="operation mode for data cleanup: filter out duplicates/non-duplicates, or annotate duplicate documents",
    )
    parser.add_argument(
        "--contents_column", type=str, required=False, help="name of the column that stores document text"
    )
    parser.add_argument(
        "--document_id_column", type=str, required=False, help="name of the column that stores document text"
    )
    parser.add_argument("--seed", type=int, required=False, help="name of the column that stores document text")
    parser.add_argument(
        "--num_permutations", type=int, required=False, help="number of permutations to use for minhash calculation"
    )
    parser.add_argument(
        "--num_bands", type=int, required=False, help="number of bands to use for band hash calculation"
    )
    parser.add_argument(
        "--num_minhashes_per_band", type=int, required=False, help="number of minhashes to use in each band"
    )
    parser.add_argument(
        "--word_shingle_size", type=int, required=False, help="number of words included in one shingle"
    )
    parser.add_argument(
        "--jaccard_similarity_threshold",
        type=float,
        required=False,
        help="jaccard similarity threshold above which two documents are similar",
    )
    parser.add_argument(
        "--num_segments",
        type=int,
        required=False,
        help="the number of segments dividing the hashing space for each band (for scalability)",
    )
    parser.add_argument(
        "--duplicate_list_location",
        type=str,
        required=False,
        help="path to the file with all the duplicate document ids",
    )

    # Single argument for service execution
    parser.add_argument(
        "--services",
        type=str,
        required=False,
        default="SignatureCalculation,ClusterAnalysis,GetDuplicateList,DataCleaning",
        help="Comma-separated list of services to run (e.g., SignatureCalculation,ClusterAnalysis,GetDuplicateList,DataCleaning)",
    )

    parser.add_argument(
        "--use_s3",
        action="store_true",
        help="use s3",
    )

    return parser.parse_args()


if __name__ == "__main__":

    # Parse command line arguments
    args = parse_args()
    # Initialize the orchestrator
    orchestrator = ServiceOrchestrator(global_params=args)
    # Launch python fuzzy dedup execution
    orchestrator.orchestrate()
